import asyncio
from collections import defaultdict, deque, OrderedDict
from collections.abc import Mapping, Set
from datetime import timedelta
from functools import partial
import inspect
import itertools
import json
import logging
import math
from numbers import Number
import operator
import os
import pickle
import random
import warnings
import weakref

import psutil
import sortedcontainers

from tlz import (
    frequencies,
    merge,
    pluck,
    merge_sorted,
    first,
    merge_with,
    valmap,
    second,
    compose,
    groupby,
    concat,
)
from tornado.ioloop import IOLoop, PeriodicCallback

import dask

from . import profile
from .batched import BatchedSend
from .comm import (
    normalize_address,
    resolve_address,
    get_address_host,
    unparse_host_port,
)
from .comm.addressing import addresses_from_user_args
from .core import rpc, connect, send_recv, clean_exception, CommClosedError
from .diagnostics.plugin import SchedulerPlugin

from .http import get_handlers
from .metrics import time
from .node import ServerNode
from . import preloading
from .proctitle import setproctitle
from .security import Security
from .utils import (
    All,
    ignoring,
    get_fileno_limit,
    log_errors,
    key_split,
    validate_key,
    no_default,
    parse_timedelta,
    parse_bytes,
    shutting_down,
    key_split_group,
    empty_context,
    tmpfile,
    format_bytes,
    format_time,
    TimeoutError,
)
from .utils_comm import scatter_to_workers, gather_from_workers, retry_operation
from .utils_perf import enable_gc_diagnosis, disable_gc_diagnosis
from . import versions as version_module

from .publish import PublishExtension
from .queues import QueueExtension
from .semaphore import SemaphoreExtension
from .recreate_exceptions import ReplayExceptionScheduler
from .lock import LockExtension
from .pubsub import PubSubSchedulerExtension
from .stealing import WorkStealing
from .variable import VariableExtension


from cudf._lib.nvtx import annotate


logger = logging.getLogger(__name__)


LOG_PDB = dask.config.get("distributed.admin.pdb-on-err")
DEFAULT_DATA_SIZE = parse_bytes(
    dask.config.get("distributed.scheduler.default-data-size")
)

DEFAULT_EXTENSIONS = [
    LockExtension,
    PublishExtension,
    ReplayExceptionScheduler,
    QueueExtension,
    VariableExtension,
    PubSubSchedulerExtension,
    SemaphoreExtension,
]

ALL_TASK_STATES = {"released", "waiting", "no-worker", "processing", "erred", "memory"}



@annotate("update_graph", domain="distributed")
def update_graph(
        self,
        client=None,
        tasks=None,
        keys=None,
        dependencies=None,
        restrictions=None,
        priority=None,
        loose_restrictions=None,
        resources=None,
        submitting_task=None,
        retries=None,
        user_priority=0,
        actors=None,
        fifo_timeout=0,
):
    """
    Add new computations to the internal dask graph

    This happens whenever the Client calls submit, map, get, or compute.
    """
    start = time()
    fifo_timeout = parse_timedelta(fifo_timeout)
    keys = set(keys)
    if len(tasks) > 1:
        self.log_event(
            ["all", client], {"action": "update_graph", "count": len(tasks)}
        )

    # Remove aliases
    for k in list(tasks):
        if tasks[k] is k:
            del tasks[k]

    dependencies = dependencies or {}

    n = 0
    while len(tasks) != n:  # walk through new tasks, cancel any bad deps
        n = len(tasks)
        for k, deps in list(dependencies.items()):
            if any(
                    dep not in self.tasks and dep not in tasks for dep in deps
            ):  # bad key
                logger.info("User asked for computation on lost data, %s", k)
                del tasks[k]
                del dependencies[k]
                if k in keys:
                    keys.remove(k)
                    self.report({"op": "cancelled-key", "key": k}, client=client)
                    self.client_releases_keys(keys=[k], client=client)

    # Remove any self-dependencies (happens on test_publish_bag() and others)
    for k, v in dependencies.items():
        deps = set(v)
        if k in deps:
            deps.remove(k)
            dependencies[k] = deps

    # Avoid computation that is already finished
    already_in_memory = set()  # tasks that are already done
    for k, v in dependencies.items():
        if v and k in self.tasks and self.tasks[k].state in ("memory", "erred"):
            already_in_memory.add(k)

    if already_in_memory:
        dependents = dask.core.reverse_dict(dependencies)
        stack = list(already_in_memory)
        done = set(already_in_memory)
        while stack:  # remove unnecessary dependencies
            key = stack.pop()
            ts = self.tasks[key]
            try:
                deps = dependencies[key]
            except KeyError:
                deps = self.dependencies[key]
            for dep in deps:
                if dep in dependents:
                    child_deps = dependents[dep]
                else:
                    child_deps = self.dependencies[dep]
                if all(d in done for d in child_deps):
                    if dep in self.tasks and dep not in done:
                        done.add(dep)
                        stack.append(dep)

        for d in done:
            tasks.pop(d, None)
            dependencies.pop(d, None)

    # Get or create task states
    stack = list(keys)
    touched_keys = set()
    touched_tasks = []
    while stack:
        k = stack.pop()
        if k in touched_keys:
            continue
        # XXX Have a method get_task_state(self, k) ?
        ts = self.tasks.get(k)
        if ts is None:
            ts = self.new_task(k, tasks.get(k), "released")
        elif not ts.run_spec:
            ts.run_spec = tasks.get(k)

        touched_keys.add(k)
        touched_tasks.append(ts)
        stack.extend(dependencies.get(k, ()))

    self.client_desires_keys(keys=keys, client=client)

    # Add dependencies
    for key, deps in dependencies.items():
        ts = self.tasks.get(key)
        if ts is None or ts.dependencies:
            continue
        for dep in deps:
            dts = self.tasks[dep]
            ts.add_dependency(dts)

    # Compute priorities
    if isinstance(user_priority, Number):
        user_priority = {k: user_priority for k in tasks}

    # Add actors
    if actors is True:
        actors = list(keys)
    for actor in actors or []:
        self.tasks[actor].actor = True

    priority = priority or dask.order.order(
        tasks
    )  # TODO: define order wrt old graph

    if submitting_task:  # sub-tasks get better priority than parent tasks
        ts = self.tasks.get(submitting_task)
        if ts is not None:
            generation = ts.priority[0] - 0.01
        else:  # super-task already cleaned up
            generation = self.generation
    elif self._last_time + fifo_timeout < start:
        self.generation += 1  # older graph generations take precedence
        generation = self.generation
        self._last_time = start
    else:
        generation = self.generation

    for key in set(priority) & touched_keys:
        ts = self.tasks[key]
        if ts.priority is None:
            ts.priority = (-(user_priority.get(key, 0)), generation, priority[key])

    # Ensure all runnables have a priority
    runnables = [ts for ts in touched_tasks if ts.run_spec]
    for ts in runnables:
        if ts.priority is None and ts.run_spec:
            ts.priority = (self.generation, 0)

    if restrictions:
        # *restrictions* is a dict keying task ids to lists of
        # restriction specifications (either worker names or addresses)
        for k, v in restrictions.items():
            if v is None:
                continue
            ts = self.tasks.get(k)
            if ts is None:
                continue
            ts.host_restrictions = set()
            ts.worker_restrictions = set()
            for w in v:
                try:
                    w = self.coerce_address(w)
                except ValueError:
                    # Not a valid address, but perhaps it's a hostname
                    ts.host_restrictions.add(w)
                else:
                    ts.worker_restrictions.add(w)

        if loose_restrictions:
            for k in loose_restrictions:
                ts = self.tasks[k]
                ts.loose_restrictions = True

    if resources:
        for k, v in resources.items():
            if v is None:
                continue
            assert isinstance(v, dict)
            ts = self.tasks.get(k)
            if ts is None:
                continue
            ts.resource_restrictions = v

    if retries:
        for k, v in retries.items():
            assert isinstance(v, int)
            ts = self.tasks.get(k)
            if ts is None:
                continue
            ts.retries = v

    # Compute recommendations
    recommendations = OrderedDict()

    for ts in sorted(runnables, key=operator.attrgetter("priority"), reverse=True):
        if ts.state == "released" and ts.run_spec:
            recommendations[ts.key] = "waiting"

    for ts in touched_tasks:
        for dts in ts.dependencies:
            if dts.exception_blame:
                ts.exception_blame = dts.exception_blame
                recommendations[ts.key] = "erred"
                break

    for plugin in self.plugins[:]:
        try:
            plugin.update_graph(
                self,
                client=client,
                tasks=tasks,
                keys=keys,
                restrictions=restrictions or {},
                dependencies=dependencies,
                priority=priority,
                loose_restrictions=loose_restrictions,
                resources=resources,
            )
        except Exception as e:
            logger.exception(e)

    self.transitions(recommendations)

    for ts in touched_tasks:
        if ts.state in ("memory", "erred"):
            self.report_on_key(ts.key, client=client)

    end = time()
    if self.digests is not None:
        self.digests["update-graph-duration"].add(end - start)




class ClientState:
    """
    A simple object holding information about a client.

    .. attribute:: client_key: str

       A unique identifier for this client.  This is generally an opaque
       string generated by the client itself.

    .. attribute:: wants_what: {TaskState}

       A set of tasks this client wants kept in memory, so that it can
       download its result when desired.  This is the reverse mapping of
       :class:`TaskState.who_wants`.

       Tasks are typically removed from this set when the corresponding
       object in the client's space (for example a ``Future`` or a Dask
       collection) gets garbage-collected.

    """

    __slots__ = ("client_key", "wants_what", "last_seen", "versions")

    def __init__(self, client, versions=None):
        self.client_key = client
        self.wants_what = set()
        self.last_seen = time()
        self.versions = versions or {}

    def __repr__(self):
        return "<Client %r>" % (self.client_key,)

    def __str__(self):
        return self.client_key


class WorkerState:
    """
    A simple object holding information about a worker.

    .. attribute:: address

       This worker's unique key.  This can be its connected address
       (such as ``'tcp://127.0.0.1:8891'``) or an alias (such as ``'alice'``).

    .. attribute:: processing: {TaskState: cost}

       A dictionary of tasks that have been submitted to this worker.
       Each task state is asssociated with the expected cost in seconds
       of running that task, summing both the task's expected computation
       time and the expected communication time of its result.

       Multiple tasks may be submitted to a worker in advance and the worker
       will run them eventually, depending on its execution resources
       (but see :doc:`work-stealing`).

       All the tasks here are in the "processing" state.

       This attribute is kept in sync with :attr:`TaskState.processing_on`.

    .. attribute:: has_what: {TaskState}

       The set of tasks which currently reside on this worker.
       All the tasks here are in the "memory" state.

       This is the reverse mapping of :class:`TaskState.who_has`.

    .. attribute:: nbytes: int

       The total memory size, in bytes, used by the tasks this worker
       holds in memory (i.e. the tasks in this worker's :attr:`has_what`).

    .. attribute:: nthreads: int

       The number of CPU threads made available on this worker.

    .. attribute:: resources: {str: Number}

       The available resources on this worker like ``{'gpu': 2}``.
       These are abstract quantities that constrain certain tasks from
       running at the same time on this worker.

    .. attribute:: used_resources: {str: Number}

       The sum of each resource used by all tasks allocated to this worker.
       The numbers in this dictionary can only be less or equal than
       those in this worker's :attr:`resources`.

    .. attribute:: occupancy: Number

       The total expected runtime, in seconds, of all tasks currently
       processing on this worker.  This is the sum of all the costs in
       this worker's :attr:`processing` dictionary.

    .. attribute:: status: str

       The current status of the worker, either ``'running'`` or ``'closed'``

    .. attribute:: nanny: str

       Address of the associated Nanny, if present

    .. attribute:: last_seen: Number

       The last time we received a heartbeat from this worker, in local
       scheduler time.

    .. attribute:: actors: {TaskState}

       A set of all TaskStates on this worker that are actors.  This only
       includes those actors whose state actually lives on this worker, not
       actors to which this worker has a reference.

    """

    # XXX need a state field to signal active/removed?

    __slots__ = (
        "actors",
        "address",
        "bandwidth",
        "extra",
        "has_what",
        "last_seen",
        "local_directory",
        "memory_limit",
        "metrics",
        "name",
        "nanny",
        "nbytes",
        "nthreads",
        "occupancy",
        "pid",
        "processing",
        "resources",
        "services",
        "status",
        "time_delay",
        "used_resources",
        "versions",
    )

    def __init__(
        self,
        address=None,
        pid=0,
        name=None,
        nthreads=0,
        memory_limit=0,
        local_directory=None,
        services=None,
        versions=None,
        nanny=None,
        extra=None,
    ):
        self.address = address
        self.pid = pid
        self.name = name
        self.nthreads = nthreads
        self.memory_limit = memory_limit
        self.local_directory = local_directory
        self.services = services or {}
        self.versions = versions or {}
        self.nanny = nanny

        self.status = "running"
        self.nbytes = 0
        self.occupancy = 0
        self.metrics = {}
        self.last_seen = 0
        self.time_delay = 0
        self.bandwidth = parse_bytes(dask.config.get("distributed.scheduler.bandwidth"))

        self.actors = set()
        self.has_what = set()
        self.processing = {}
        self.resources = {}
        self.used_resources = {}

        self.extra = extra or {}

    def __hash__(self):
        return hash(self.address)

    def __eq__(self, other):
        return type(self) == type(other) and self.address == other.address

    @property
    def host(self):
        return get_address_host(self.address)

    def clean(self):
        """ Return a version of this object that is appropriate for serialization """
        ws = WorkerState(
            address=self.address,
            pid=self.pid,
            name=self.name,
            nthreads=self.nthreads,
            memory_limit=self.memory_limit,
            local_directory=self.local_directory,
            services=self.services,
            nanny=self.nanny,
            extra=self.extra,
        )
        ws.processing = {ts.key for ts in self.processing}
        return ws

    def __repr__(self):
        return "<Worker %r, name: %s, memory: %d, processing: %d>" % (
            self.address,
            self.name,
            len(self.has_what),
            len(self.processing),
        )

    def identity(self):
        return {
            "type": "Worker",
            "id": self.name,
            "host": self.host,
            "resources": self.resources,
            "local_directory": self.local_directory,
            "name": self.name,
            "nthreads": self.nthreads,
            "memory_limit": self.memory_limit,
            "last_seen": self.last_seen,
            "services": self.services,
            "metrics": self.metrics,
            "nanny": self.nanny,
            **self.extra,
        }

    @property
    def ncores(self):
        warnings.warn("WorkerState.ncores has moved to WorkerState.nthreads")
        return self.nthreads

        
cdef class TaskState:

    cdef public:
        cdef object key
        cdef object run_spec
        cdef object _state
        cdef object exception
        cdef object traceback
        cdef object prefix
        cdef object exception_blame
        cdef int suspicious
        cdef int retries
        cdef object nbytes
        cdef object priority
        cdef set who_wants
        cdef set dependencies
        cdef set dependents
        cdef set waiting_on
        cdef set waiters
        cdef set who_has
        cdef object processing_on
        cdef object has_lost_dependencies
        cdef object host_restrictions
        cdef object worker_restrictions
        cdef object resource_restrictions
        cdef object loose_restrictions
        cdef object actor
        cdef object type
        cdef object group_key
        cdef object group

    def __init__(self, key, run_spec):
        self.key = key
        self.run_spec = run_spec
        self._state = None
        self.exception = self.traceback = self.exception_blame = None
        self.suspicious = self.retries = 0
        self.nbytes = None
        self.priority = None
        self.who_wants = set()
        self.dependencies = set()
        self.dependents = set()
        self.waiting_on = set()
        self.waiters = set()
        self.who_has = set()
        self.processing_on = None
        self.has_lost_dependencies = False
        self.host_restrictions = None
        self.worker_restrictions = None
        self.resource_restrictions = None
        self.loose_restrictions = False
        self.actor = None
        self.type = None
        self.group_key = key_split_group(key)
        self.group = None

    
    @property
    def state(self) -> str:
        return self._state

    @property
    def prefix_key(self):
        return self.prefix.name

    @state.setter
    def state(self, value: str):
        self.group.states[self._state] -= 1
        self.group.states[value] += 1
        self._state = value

    def add_dependency(self, other: "TaskState"):
        """ Add another task as a dependency of this task """
        self.dependencies.add(other)
        self.group.dependencies.add(other.group)
        other.dependents.add(self)

    def get_nbytes(self) -> int:
        nbytes = self.nbytes
        return nbytes if nbytes is not None else DEFAULT_DATA_SIZE

    def set_nbytes(self, nbytes: int):
        old_nbytes = self.nbytes
        diff = nbytes - (old_nbytes or 0)
        self.group.nbytes_total += diff
        self.group.nbytes_in_memory += diff
        for ws in self.who_has:
            ws.nbytes += diff
        self.nbytes = nbytes

    def __repr__(self):
        return "<Task %r %s>" % (self.key, self.state)

    def validate(self):
        try:
            for cs in self.who_wants:
                assert isinstance(cs, ClientState), (repr(cs), self.who_wants)
            for ws in self.who_has:
                assert isinstance(ws, WorkerState), (repr(ws), self.who_has)
            for ts in self.dependencies:
                assert isinstance(ts, TaskState), (repr(ts), self.dependencies)
            for ts in self.dependents:
                assert isinstance(ts, TaskState), (repr(ts), self.dependents)
            validate_task_state(self)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()

        
def validate_task_state(ts):
    """
    Validate the given TaskState.
    """
    assert ts.state in ALL_TASK_STATES or ts.state == "forgotten", ts

    if ts.waiting_on:
        assert ts.waiting_on.issubset(ts.dependencies), (
            "waiting not subset of dependencies",
            str(ts.waiting_on),
            str(ts.dependencies),
        )
    if ts.waiters:
        assert ts.waiters.issubset(ts.dependents), (
            "waiters not subset of dependents",
            str(ts.waiters),
            str(ts.dependents),
        )

    for dts in ts.waiting_on:
        assert not dts.who_has, ("waiting on in-memory dep", str(ts), str(dts))
        assert dts.state != "released", ("waiting on released dep", str(ts), str(dts))
    for dts in ts.dependencies:
        assert ts in dts.dependents, (
            "not in dependency's dependents",
            str(ts),
            str(dts),
            str(dts.dependents),
        )
        if ts.state in ("waiting", "processing"):
            assert dts in ts.waiting_on or dts.who_has, (
                "dep missing",
                str(ts),
                str(dts),
            )
        assert dts.state != "forgotten"

    for dts in ts.waiters:
        assert dts.state in ("waiting", "processing"), (
            "waiter not in play",
            str(ts),
            str(dts),
        )
    for dts in ts.dependents:
        assert ts in dts.dependencies, (
            "not in dependent's dependencies",
            str(ts),
            str(dts),
            str(dts.dependencies),
        )
        assert dts.state != "forgotten"

    assert (ts.processing_on is not None) == (ts.state == "processing")
    assert bool(ts.who_has) == (ts.state == "memory"), (ts, ts.who_has)

    if ts.state == "processing":
        assert all(dts.who_has for dts in ts.dependencies), (
            "task processing without all deps",
            str(ts),
            str(ts.dependencies),
        )
        assert not ts.waiting_on

    if ts.who_has:
        assert ts.waiters or ts.who_wants, (
            "unneeded task in memory",
            str(ts),
            str(ts.who_has),
        )
        if ts.run_spec:  # was computed
            assert ts.type
            assert isinstance(ts.type, str)
        assert not any(ts in dts.waiting_on for dts in ts.dependents)
        for ws in ts.who_has:
            assert ts in ws.has_what, (
                "not in who_has' has_what",
                str(ts),
                str(ws),
                str(ws.has_what),
            )

    if ts.who_wants:
        for cs in ts.who_wants:
            assert ts in cs.wants_what, (
                "not in who_wants' wants_what",
                str(ts),
                str(cs),
                str(cs.wants_what),
            )

    if ts.actor:
        if ts.state == "memory":
            assert sum([ts in ws.actors for ws in ts.who_has]) == 1
        if ts.state == "processing":
            assert ts in ts.processing_on.actors
