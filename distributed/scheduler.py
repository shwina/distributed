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
from .scheduler_cy import (
    update_graph as update_graph_cy,
    ClientState,
    WorkerState,
    TaskState,
    validate_task_state
)

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


class TaskGroup:
    """ Collection tracking all tasks within a group

    Keys often have a structure like ``("x-123", 0)``
    A group takes the first section, like ``"x-123"``

    .. attribute:: name: str

       The name of a group of tasks.
       For a task like ``("x-123", 0)`` this is the text ``"x-123"``

    .. attribute:: states: Dict[str, int]

       The number of tasks in each state,
       like ``{"memory": 10, "processing": 3, "released": 4, ...}``

    .. attribute:: dependencies: Set[TaskGroup]

       The other TaskGroups on which this one depends

    .. attribute:: nbytes_total: int

       The total number of bytes that this task group has produced

    .. attribute:: nbytes_in_memory: int

       The number of bytes currently stored by this TaskGroup

    .. attribute:: duration: float

       The total amount of time spent on all tasks in this TaskGroup

    .. attribute:: types: Set[str]

       The result types of this TaskGroup

    See also
    --------
    TaskPrefix
    """

    def __init__(self, name):
        self.name = name
        self.states = {state: 0 for state in ALL_TASK_STATES}
        self.states["forgotten"] = 0
        self.dependencies = set()
        self.nbytes_total = 0
        self.nbytes_in_memory = 0
        self.duration = 0
        self.types = set()

    def add(self, ts):
        # self.tasks.add(ts)
        self.states[ts.state] += 1
        ts.group = self

    def __repr__(self):
        return (
            "<"
            + (self.name or "no-group")
            + ": "
            + ", ".join(
                "%s: %d" % (k, v) for (k, v) in sorted(self.states.items()) if v
            )
            + ">"
        )

    def __len__(self):
        return sum(self.states.values())


class TaskPrefix:
    """ Collection tracking all tasks within a group

    Keys often have a structure like ``("x-123", 0)``
    A group takes the first section, like ``"x"``

    .. attribute:: name: str

       The name of a group of tasks.
       For a task like ``("x-123", 0)`` this is the text ``"x"``

    .. attribute:: states: Dict[str, int]

       The number of tasks in each state,
       like ``{"memory": 10, "processing": 3, "released": 4, ...}``

    .. attribute:: duration_average: float

       An exponentially weighted moving average duration of all tasks with this prefix

    .. attribute:: suspicious: int

       Numbers of times a task was marked as suspicious with this prefix


    See Also
    --------
    TaskGroup
    """

    def __init__(self, name):
        self.name = name
        self.groups = []
        if self.name in dask.config.get("distributed.scheduler.default-task-durations"):
            self.duration_average = parse_timedelta(
                dask.config.get("distributed.scheduler.default-task-durations")[
                    self.name
                ]
            )
        else:
            self.duration_average = None
        self.suspicious = 0

    @property
    def states(self):
        return merge_with(sum, [g.states for g in self.groups])

    @property
    def active(self):
        return [
            g
            for g in self.groups
            if any(v != 0 for k, v in g.states.items() if k != "forgotten")
        ]

    @property
    def active_states(self):
        return merge_with(sum, [g.states for g in self.active])

    def __repr__(self):
        return (
            "<"
            + self.name
            + ": "
            + ", ".join(
                "%s: %d" % (k, v) for (k, v) in sorted(self.states.items()) if v
            )
            + ">"
        )

    @property
    def nbytes_in_memory(self):
        return sum(tg.nbytes_in_memory for tg in self.groups)

    @property
    def nbytes_total(self):
        return sum(tg.nbytes_total for tg in self.groups)

    def __len__(self):
        return sum(map(len, self.groups))

    @property
    def duration(self):
        return sum(tg.duration for tg in self.groups)

    @property
    def types(self):
        return set().union(*[tg.types for tg in self.groups])


class _StateLegacyMapping(Mapping):
    """
    A mapping interface mimicking the former Scheduler state dictionaries.
    """

    def __init__(self, states, accessor):
        self._states = states
        self._accessor = accessor

    def __iter__(self):
        return iter(self._states)

    def __len__(self):
        return len(self._states)

    def __getitem__(self, key):
        return self._accessor(self._states[key])

    def __repr__(self):
        return "%s(%s)" % (self.__class__, dict(self))


class _OptionalStateLegacyMapping(_StateLegacyMapping):
    """
    Similar to _StateLegacyMapping, but a false-y value is interpreted
    as a missing key.
    """

    # For tasks etc.

    def __iter__(self):
        accessor = self._accessor
        for k, v in self._states.items():
            if accessor(v):
                yield k

    def __len__(self):
        accessor = self._accessor
        return sum(bool(accessor(v)) for v in self._states.values())

    def __getitem__(self, key):
        v = self._accessor(self._states[key])
        if v:
            return v
        else:
            raise KeyError


class _StateLegacySet(Set):
    """
    Similar to _StateLegacyMapping, but exposes a set containing
    all values with a true value.
    """

    # For loose_restrictions

    def __init__(self, states, accessor):
        self._states = states
        self._accessor = accessor

    def __iter__(self):
        return (k for k, v in self._states.items() if self._accessor(v))

    def __len__(self):
        return sum(map(bool, map(self._accessor, self._states.values())))

    def __contains__(self, k):
        st = self._states.get(k)
        return st is not None and bool(self._accessor(st))

    def __repr__(self):
        return "%s(%s)" % (self.__class__, set(self))


def _legacy_task_key_set(tasks):
    """
    Transform a set of task states into a set of task keys.
    """
    return {ts.key for ts in tasks}


def _legacy_client_key_set(clients):
    """
    Transform a set of client states into a set of client keys.
    """
    return {cs.client_key for cs in clients}


def _legacy_worker_key_set(workers):
    """
    Transform a set of worker states into a set of worker keys.
    """
    return {ws.address for ws in workers}


def _legacy_task_key_dict(task_dict):
    """
    Transform a dict of {task state: value} into a dict of {task key: value}.
    """
    return {ts.key: value for ts, value in task_dict.items()}


def _task_key_or_none(task):
    return task.key if task is not None else None


class Scheduler(ServerNode):
    """ Dynamic distributed task scheduler

    The scheduler tracks the current state of workers, data, and computations.
    The scheduler listens for events and responds by controlling workers
    appropriately.  It continuously tries to use the workers to execute an ever
    growing dask graph.

    All events are handled quickly, in linear time with respect to their input
    (which is often of constant size) and generally within a millisecond.  To
    accomplish this the scheduler tracks a lot of state.  Every operation
    maintains the consistency of this state.

    The scheduler communicates with the outside world through Comm objects.
    It maintains a consistent and valid view of the world even when listening
    to several clients at once.

    A Scheduler is typically started either with the ``dask-scheduler``
    executable::

         $ dask-scheduler
         Scheduler started at 127.0.0.1:8786

    Or within a LocalCluster a Client starts up without connection
    information::

        >>> c = Client()  # doctest: +SKIP
        >>> c.cluster.scheduler  # doctest: +SKIP
        Scheduler(...)

    Users typically do not interact with the scheduler directly but rather with
    the client object ``Client``.

    **State**

    The scheduler contains the following state variables.  Each variable is
    listed along with what it stores and a brief description.

    * **tasks:** ``{task key: TaskState}``
        Tasks currently known to the scheduler
    * **unrunnable:** ``{TaskState}``
        Tasks in the "no-worker" state

    * **workers:** ``{worker key: WorkerState}``
        Workers currently connected to the scheduler
    * **idle:** ``{WorkerState}``:
        Set of workers that are not fully utilized
    * **saturated:** ``{WorkerState}``:
        Set of workers that are not over-utilized

    * **host_info:** ``{hostname: dict}``:
        Information about each worker host

    * **clients:** ``{client key: ClientState}``
        Clients currently connected to the scheduler

    * **services:** ``{str: port}``:
        Other services running on this scheduler, like Bokeh
    * **loop:** ``IOLoop``:
        The running Tornado IOLoop
    * **client_comms:** ``{client key: Comm}``
        For each client, a Comm object used to receive task requests and
        report task status updates.
    * **stream_comms:** ``{worker key: Comm}``
        For each worker, a Comm object from which we both accept stimuli and
        report results
    * **task_duration:** ``{key-prefix: time}``
        Time we expect certain functions to take, e.g. ``{'sum': 0.25}``
    """

    default_port = 8786
    _instances = weakref.WeakSet()

    def __init__(
        self,
        loop=None,
        delete_interval="500ms",
        synchronize_worker_interval="60s",
        services=None,
        service_kwargs=None,
        allowed_failures=None,
        extensions=None,
        validate=None,
        scheduler_file=None,
        security=None,
        worker_ttl=None,
        idle_timeout=None,
        interface=None,
        host=None,
        port=0,
        protocol=None,
        dashboard_address=None,
        dashboard=None,
        http_prefix="/",
        preload=None,
        preload_argv=(),
        plugins=(),
        **kwargs
    ):
        self._setup_logging(logger)

        # Attributes
        if allowed_failures is None:
            allowed_failures = dask.config.get("distributed.scheduler.allowed-failures")
        self.allowed_failures = allowed_failures
        if validate is None:
            validate = dask.config.get("distributed.scheduler.validate")
        self.validate = validate
        self.status = None
        self.proc = psutil.Process()
        self.delete_interval = parse_timedelta(delete_interval, default="ms")
        self.synchronize_worker_interval = parse_timedelta(
            synchronize_worker_interval, default="ms"
        )
        self.digests = None
        self.service_specs = services or {}
        self.service_kwargs = service_kwargs or {}
        self.services = {}
        self.scheduler_file = scheduler_file
        worker_ttl = worker_ttl or dask.config.get("distributed.scheduler.worker-ttl")
        self.worker_ttl = parse_timedelta(worker_ttl) if worker_ttl else None
        idle_timeout = idle_timeout or dask.config.get(
            "distributed.scheduler.idle-timeout"
        )
        if idle_timeout:
            self.idle_timeout = parse_timedelta(idle_timeout)
        else:
            self.idle_timeout = None
        self.time_started = time()
        self._lock = asyncio.Lock()
        self.bandwidth = parse_bytes(dask.config.get("distributed.scheduler.bandwidth"))
        self.bandwidth_workers = defaultdict(float)
        self.bandwidth_types = defaultdict(float)

        if not preload:
            preload = dask.config.get("distributed.scheduler.preload")
        if not preload_argv:
            preload_argv = dask.config.get("distributed.scheduler.preload-argv")
        self.preload = preload
        self.preload_argv = preload_argv
        self._preload_modules = preloading.on_creation(self.preload)

        self.security = security or Security()
        assert isinstance(self.security, Security)
        self.connection_args = self.security.get_connection_args("scheduler")

        self._start_address = addresses_from_user_args(
            host=host,
            port=port,
            interface=interface,
            protocol=protocol,
            security=security,
            default_port=self.default_port,
        )

        routes = get_handlers(
            server=self,
            modules=dask.config.get("distributed.scheduler.http.routes"),
            prefix=http_prefix,
        )
        self.start_http_server(routes, dashboard_address, default_port=8787)

        if dashboard:
            try:
                import distributed.dashboard.scheduler
            except ImportError:
                logger.debug("To start diagnostics web server please install Bokeh")
            else:
                distributed.dashboard.scheduler.connect(
                    self.http_application, self.http_server, self, prefix=http_prefix,
                )

        # Communication state
        self.loop = loop or IOLoop.current()
        self.client_comms = dict()
        self.stream_comms = dict()
        self._worker_coroutines = []
        self._ipython_kernel = None

        # Task state
        self.tasks = dict()
        self.task_groups = dict()
        self.task_prefixes = dict()
        for old_attr, new_attr, wrap in [
            ("priority", "priority", None),
            ("dependencies", "dependencies", _legacy_task_key_set),
            ("dependents", "dependents", _legacy_task_key_set),
            ("retries", "retries", None),
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _StateLegacyMapping(self.tasks, func))

        for old_attr, new_attr, wrap in [
            ("nbytes", "nbytes", None),
            ("who_wants", "who_wants", _legacy_client_key_set),
            ("who_has", "who_has", _legacy_worker_key_set),
            ("waiting", "waiting_on", _legacy_task_key_set),
            ("waiting_data", "waiters", _legacy_task_key_set),
            ("rprocessing", "processing_on", None),
            ("host_restrictions", "host_restrictions", None),
            ("worker_restrictions", "worker_restrictions", None),
            ("resource_restrictions", "resource_restrictions", None),
            ("suspicious_tasks", "suspicious", None),
            ("exceptions", "exception", None),
            ("tracebacks", "traceback", None),
            ("exceptions_blame", "exception_blame", _task_key_or_none),
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _OptionalStateLegacyMapping(self.tasks, func))

        for old_attr, new_attr, wrap in [
            ("loose_restrictions", "loose_restrictions", None)
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _StateLegacySet(self.tasks, func))

        self.generation = 0
        self._last_client = None
        self._last_time = 0
        self.unrunnable = set()

        self.n_tasks = 0
        self.task_metadata = dict()
        self.datasets = dict()

        # Prefix-keyed containers
        self.unknown_durations = defaultdict(set)

        # Client state
        self.clients = dict()
        for old_attr, new_attr, wrap in [
            ("wants_what", "wants_what", _legacy_task_key_set)
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _StateLegacyMapping(self.clients, func))
        self.clients["fire-and-forget"] = ClientState("fire-and-forget")

        # Worker state
        self.workers = sortedcontainers.SortedDict()
        for old_attr, new_attr, wrap in [
            ("nthreads", "nthreads", None),
            ("worker_bytes", "nbytes", None),
            ("worker_resources", "resources", None),
            ("used_resources", "used_resources", None),
            ("occupancy", "occupancy", None),
            ("worker_info", "metrics", None),
            ("processing", "processing", _legacy_task_key_dict),
            ("has_what", "has_what", _legacy_task_key_set),
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _StateLegacyMapping(self.workers, func))

        self.idle = sortedcontainers.SortedSet(key=operator.attrgetter("address"))
        self.saturated = set()

        self.total_nthreads = 0
        self.total_occupancy = 0
        self.host_info = defaultdict(dict)
        self.resources = defaultdict(dict)
        self.aliases = dict()

        self._task_state_collections = [self.unrunnable]

        self._worker_collections = [
            self.workers,
            self.host_info,
            self.resources,
            self.aliases,
        ]

        self.extensions = {}
        self.plugins = list(plugins)
        self.transition_log = deque(
            maxlen=dask.config.get("distributed.scheduler.transition-log-length")
        )
        self.log = deque(
            maxlen=dask.config.get("distributed.scheduler.transition-log-length")
        )
        self.worker_plugins = []

        worker_handlers = {
            "task-finished": self.handle_task_finished,
            "task-erred": self.handle_task_erred,
            "release": self.handle_release_data,
            "release-worker-data": self.release_worker_data,
            "add-keys": self.add_keys,
            "missing-data": self.handle_missing_data,
            "long-running": self.handle_long_running,
            "reschedule": self.reschedule,
            "keep-alive": lambda *args, **kwargs: None,
        }

        client_handlers = {
            "update-graph": self.update_graph,
            "client-desires-keys": self.client_desires_keys,
            "update-data": self.update_data,
            "report-key": self.report_on_key,
            "client-releases-keys": self.client_releases_keys,
            "heartbeat-client": self.client_heartbeat,
            "close-client": self.remove_client,
            "restart": self.restart,
        }

        self.handlers = {
            "register-client": self.add_client,
            "scatter": self.scatter,
            "register-worker": self.add_worker,
            "unregister": self.remove_worker,
            "gather": self.gather,
            "cancel": self.stimulus_cancel,
            "retry": self.stimulus_retry,
            "feed": self.feed,
            "terminate": self.close,
            "broadcast": self.broadcast,
            "proxy": self.proxy,
            "ncores": self.get_ncores,
            "has_what": self.get_has_what,
            "who_has": self.get_who_has,
            "processing": self.get_processing,
            "call_stack": self.get_call_stack,
            "profile": self.get_profile,
            "performance_report": self.performance_report,
            "get_logs": self.get_logs,
            "logs": self.get_logs,
            "worker_logs": self.get_worker_logs,
            "nbytes": self.get_nbytes,
            "versions": self.versions,
            "add_keys": self.add_keys,
            "rebalance": self.rebalance,
            "replicate": self.replicate,
            "start_ipython": self.start_ipython,
            "run_function": self.run_function,
            "update_data": self.update_data,
            "set_resources": self.add_resources,
            "retire_workers": self.retire_workers,
            "get_metadata": self.get_metadata,
            "set_metadata": self.set_metadata,
            "heartbeat_worker": self.heartbeat_worker,
            "get_task_status": self.get_task_status,
            "get_task_stream": self.get_task_stream,
            "register_worker_plugin": self.register_worker_plugin,
            "adaptive_target": self.adaptive_target,
            "workers_to_close": self.workers_to_close,
            "subscribe_worker_status": self.subscribe_worker_status,
        }

        self._transitions = {
            ("released", "waiting"): self.transition_released_waiting,
            ("waiting", "released"): self.transition_waiting_released,
            ("waiting", "processing"): self.transition_waiting_processing,
            ("waiting", "memory"): self.transition_waiting_memory,
            ("processing", "released"): self.transition_processing_released,
            ("processing", "memory"): self.transition_processing_memory,
            ("processing", "erred"): self.transition_processing_erred,
            ("no-worker", "released"): self.transition_no_worker_released,
            ("no-worker", "waiting"): self.transition_no_worker_waiting,
            ("released", "forgotten"): self.transition_released_forgotten,
            ("memory", "forgotten"): self.transition_memory_forgotten,
            ("erred", "forgotten"): self.transition_released_forgotten,
            ("erred", "released"): self.transition_erred_released,
            ("memory", "released"): self.transition_memory_released,
            ("released", "erred"): self.transition_released_erred,
        }

        connection_limit = get_fileno_limit() / 2

        super(Scheduler, self).__init__(
            handlers=self.handlers,
            stream_handlers=merge(worker_handlers, client_handlers),
            io_loop=self.loop,
            connection_limit=connection_limit,
            deserialize=False,
            connection_args=self.connection_args,
            **kwargs
        )

        if self.worker_ttl:
            pc = PeriodicCallback(self.check_worker_ttl, self.worker_ttl)
            self.periodic_callbacks["worker-ttl"] = pc

        if self.idle_timeout:
            pc = PeriodicCallback(self.check_idle, self.idle_timeout / 4)
            self.periodic_callbacks["idle-timeout"] = pc

        if extensions is None:
            extensions = list(DEFAULT_EXTENSIONS)
            if dask.config.get("distributed.scheduler.work-stealing"):
                extensions.append(WorkStealing)
        for ext in extensions:
            ext(self)

        setproctitle("dask-scheduler [not started]")
        Scheduler._instances.add(self)

    ##################
    # Administration #
    ##################

    def __repr__(self):
        return '<Scheduler: "%s" processes: %d cores: %d>' % (
            self.address,
            len(self.workers),
            self.total_nthreads,
        )

    def identity(self, comm=None):
        """ Basic information about ourselves and our cluster """
        d = {
            "type": type(self).__name__,
            "id": str(self.id),
            "address": self.address,
            "services": {key: v.port for (key, v) in self.services.items()},
            "workers": {
                worker.address: worker.identity() for worker in self.workers.values()
            },
        }
        return d

    def get_worker_service_addr(self, worker, service_name, protocol=False):
        """
        Get the (host, port) address of the named service on the *worker*.
        Returns None if the service doesn't exist.

        Parameters
        ----------
        worker : address
        service_name : str
            Common services include 'bokeh' and 'nanny'
        protocol : boolean
            Whether or not to include a full address with protocol (True)
            or just a (host, port) pair
        """
        ws = self.workers[worker]
        port = ws.services.get(service_name)
        if port is None:
            return None
        elif protocol:
            return "%(protocol)s://%(host)s:%(port)d" % {
                "protocol": ws.address.split("://")[0],
                "host": ws.host,
                "port": port,
            }
        else:
            return ws.host, port

    async def start(self):
        """ Clear out old state and restart all running coroutines """

        await super().start()

        enable_gc_diagnosis()

        self.clear_task_state()

        with ignoring(AttributeError):
            for c in self._worker_coroutines:
                c.cancel()

        if self.status != "running":
            for addr in self._start_address:
                await self.listen(addr, **self.security.get_listen_args("scheduler"))
                self.ip = get_address_host(self.listen_address)
                listen_ip = self.ip

                if listen_ip == "0.0.0.0":
                    listen_ip = ""

            if self.address.startswith("inproc://"):
                listen_ip = "localhost"

            # Services listen on all addresses
            self.start_services(listen_ip)

            self.status = "running"
            for listener in self.listeners:
                logger.info("  Scheduler at: %25s", listener.contact_address)
            for k, v in self.services.items():
                logger.info("%11s at: %25s", k, "%s:%d" % (listen_ip, v.port))

            self.loop.add_callback(self.reevaluate_occupancy)

        if self.scheduler_file:
            with open(self.scheduler_file, "w") as f:
                json.dump(self.identity(), f, indent=2)

            fn = self.scheduler_file  # remove file when we close the process

            def del_scheduler_file():
                if os.path.exists(fn):
                    os.remove(fn)

            weakref.finalize(self, del_scheduler_file)

        await preloading.on_start(self._preload_modules, self, argv=self.preload_argv)

        await asyncio.gather(*[plugin.start(self) for plugin in self.plugins])

        self.start_periodic_callbacks()

        setproctitle("dask-scheduler [%s]" % (self.address,))
        return self

    async def close(self, comm=None, fast=False, close_workers=False):
        """ Send cleanup signal to all coroutines then wait until finished

        See Also
        --------
        Scheduler.cleanup
        """
        if self.status.startswith("clos"):
            await self.finished()
            return
        self.status = "closing"

        logger.info("Scheduler closing...")
        setproctitle("dask-scheduler [closing]")

        await preloading.on_teardown(self._preload_modules, self)

        if close_workers:
            await self.broadcast(msg={"op": "close_gracefully"}, nanny=True)
            for worker in self.workers:
                self.worker_send(worker, {"op": "close"})
            for i in range(20):  # wait a second for send signals to clear
                if self.workers:
                    await asyncio.sleep(0.05)
                else:
                    break

        await asyncio.gather(*[plugin.close() for plugin in self.plugins])

        for pc in self.periodic_callbacks.values():
            pc.stop()
        self.periodic_callbacks.clear()

        self.stop_services()

        for ext in self.extensions.values():
            with ignoring(AttributeError):
                ext.teardown()
        logger.info("Scheduler closing all comms")

        futures = []
        for w, comm in list(self.stream_comms.items()):
            if not comm.closed():
                comm.send({"op": "close", "report": False})
                comm.send({"op": "close-stream"})
            with ignoring(AttributeError):
                futures.append(comm.close())

        for future in futures:  # TODO: do all at once
            await future

        for comm in self.client_comms.values():
            comm.abort()

        await self.rpc.close()

        self.status = "closed"
        self.stop()
        await super(Scheduler, self).close()

        setproctitle("dask-scheduler [closed]")
        disable_gc_diagnosis()

    async def close_worker(self, stream=None, worker=None, safe=None):
        """ Remove a worker from the cluster

        This both removes the worker from our local state and also sends a
        signal to the worker to shut down.  This works regardless of whether or
        not the worker has a nanny process restarting it
        """
        logger.info("Closing worker %s", worker)
        with log_errors():
            self.log_event(worker, {"action": "close-worker"})
            nanny_addr = self.workers[worker].nanny
            address = nanny_addr or worker

            self.worker_send(worker, {"op": "close", "report": False})
            self.remove_worker(address=worker, safe=safe)

    ###########
    # Stimuli #
    ###########

    def heartbeat_worker(
        self,
        comm=None,
        address=None,
        resolve_address=True,
        now=None,
        resources=None,
        host_info=None,
        metrics=None,
    ):
        address = self.coerce_address(address, resolve_address)
        address = normalize_address(address)
        if address not in self.workers:
            return {"status": "missing"}

        host = get_address_host(address)
        local_now = time()
        now = now or time()
        assert metrics
        host_info = host_info or {}

        self.host_info[host]["last-seen"] = local_now
        frac = 1 / len(self.workers)
        self.bandwidth = (
            self.bandwidth * (1 - frac) + metrics["bandwidth"]["total"] * frac
        )
        for other, (bw, count) in metrics["bandwidth"]["workers"].items():
            if (address, other) not in self.bandwidth_workers:
                self.bandwidth_workers[address, other] = bw / count
            else:
                alpha = (1 - frac) ** count
                self.bandwidth_workers[address, other] = self.bandwidth_workers[
                    address, other
                ] * alpha + bw * (1 - alpha)
        for typ, (bw, count) in metrics["bandwidth"]["types"].items():
            if typ not in self.bandwidth_types:
                self.bandwidth_types[typ] = bw / count
            else:
                alpha = (1 - frac) ** count
                self.bandwidth_types[typ] = self.bandwidth_types[typ] * alpha + bw * (
                    1 - alpha
                )

        ws = self.workers[address]

        ws.last_seen = time()

        if metrics:
            ws.metrics = metrics

        if host_info:
            self.host_info[host].update(host_info)

        delay = time() - now
        ws.time_delay = delay

        if resources:
            self.add_resources(worker=address, resources=resources)

        self.log_event(address, merge({"action": "heartbeat"}, metrics))

        return {
            "status": "OK",
            "time": time(),
            "heartbeat-interval": heartbeat_interval(len(self.workers)),
        }

    async def add_worker(
        self,
        comm=None,
        address=None,
        keys=(),
        nthreads=None,
        name=None,
        resolve_address=True,
        nbytes=None,
        types=None,
        now=None,
        resources=None,
        host_info=None,
        memory_limit=None,
        metrics=None,
        pid=0,
        services=None,
        local_directory=None,
        versions=None,
        nanny=None,
        extra=None,
    ):
        """ Add a new worker to the cluster """
        with log_errors():
            address = self.coerce_address(address, resolve_address)
            address = normalize_address(address)
            host = get_address_host(address)

            ws = self.workers.get(address)
            if ws is not None:
                raise ValueError("Worker already exists %s" % ws)

            if name in self.aliases:
                msg = {
                    "status": "error",
                    "message": "name taken, %s" % name,
                    "time": time(),
                }
                if comm:
                    await comm.write(msg)
                return

            self.workers[address] = ws = WorkerState(
                address=address,
                pid=pid,
                nthreads=nthreads,
                memory_limit=memory_limit,
                name=name,
                local_directory=local_directory,
                services=services,
                versions=versions,
                nanny=nanny,
                extra=extra,
            )

            if "addresses" not in self.host_info[host]:
                self.host_info[host].update({"addresses": set(), "nthreads": 0})

            self.host_info[host]["addresses"].add(address)
            self.host_info[host]["nthreads"] += nthreads

            self.total_nthreads += nthreads
            self.aliases[name] = address

            response = self.heartbeat_worker(
                address=address,
                resolve_address=resolve_address,
                now=now,
                resources=resources,
                host_info=host_info,
                metrics=metrics,
            )

            # Do not need to adjust self.total_occupancy as self.occupancy[ws] cannot exist before this.
            self.check_idle_saturated(ws)

            # for key in keys:  # TODO
            #     self.mark_key_in_memory(key, [address])

            self.stream_comms[address] = BatchedSend(interval="5ms", loop=self.loop)

            if ws.nthreads > len(ws.processing):
                self.idle.add(ws)

            for plugin in self.plugins[:]:
                try:
                    plugin.add_worker(scheduler=self, worker=address)
                except Exception as e:
                    logger.exception(e)

            if nbytes:
                for key in nbytes:
                    ts = self.tasks.get(key)
                    if ts is not None and ts.state in ("processing", "waiting"):
                        recommendations = self.transition(
                            key,
                            "memory",
                            worker=address,
                            nbytes=nbytes[key],
                            typename=types[key],
                        )
                        self.transitions(recommendations)

            recommendations = {}
            for ts in list(self.unrunnable):
                valid = self.valid_workers(ts)
                if valid is True or ws in valid:
                    recommendations[ts.key] = "waiting"

            if recommendations:
                self.transitions(recommendations)

            self.log_event(address, {"action": "add-worker"})
            self.log_event("all", {"action": "add-worker", "worker": address})
            logger.info("Register worker %s", ws)

            msg = {
                "status": "OK",
                "time": time(),
                "heartbeat-interval": heartbeat_interval(len(self.workers)),
                "worker-plugins": self.worker_plugins,
            }

            version_warning = version_module.error_message(
                version_module.get_versions(),
                merge(
                    {w: ws.versions for w, ws in self.workers.items()},
                    {c: cs.versions for c, cs in self.clients.items() if cs.versions},
                ),
                versions,
                client_name="This Worker",
            )
            if version_warning:
                msg["warning"] = version_warning

            if comm:
                await comm.write(msg)
            await self.handle_worker(comm=comm, worker=address)

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
        return update_graph_cy(
            self,
            client=client,
            tasks=tasks,
            keys=keys,
            dependencies=dependencies,
            restrictions=restrictions,
            priority=priority,
            loose_restrictions=loose_restrictions,
            resources=resources,
            submitting_task=submitting_task,
            retries=retries,
            user_priority=user_priority,
            actors=actors,
            fifo_timeout=fifo_timeout,
        )
        # TODO: balance workers

    def new_task(self, key, spec, state):
        """ Create a new task, and associated states """
        ts = TaskState(key, spec)
        ts._state = state
        prefix_key = key_split(key)
        try:
            tp = self.task_prefixes[prefix_key]
        except KeyError:
            tp = self.task_prefixes[prefix_key] = TaskPrefix(prefix_key)
        ts.prefix = tp

        group_key = ts.group_key
        try:
            tg = self.task_groups[group_key]
        except KeyError:
            tg = self.task_groups[group_key] = TaskGroup(group_key)
            tg.prefix = tp
            tp.groups.append(tg)
        tg.add(ts)
        self.tasks[key] = ts
        return ts

    def stimulus_task_finished(self, key=None, worker=None, **kwargs):
        """ Mark that a task has finished execution on a particular worker """
        logger.debug("Stimulus task finished %s, %s", key, worker)

        ts = self.tasks.get(key)
        if ts is None:
            return {}
        ws = self.workers[worker]

        if ts.state == "processing":
            recommendations = self.transition(key, "memory", worker=worker, **kwargs)

            if ts.state == "memory":
                assert ws in ts.who_has
        else:
            logger.debug(
                "Received already computed task, worker: %s, state: %s"
                ", key: %s, who_has: %s",
                worker,
                ts.state,
                key,
                ts.who_has,
            )
            if ws not in ts.who_has:
                self.worker_send(worker, {"op": "release-task", "key": key})
            recommendations = {}

        return recommendations

    def stimulus_task_erred(
        self, key=None, worker=None, exception=None, traceback=None, **kwargs
    ):
        """ Mark that a task has erred on a particular worker """
        logger.debug("Stimulus task erred %s, %s", key, worker)

        ts = self.tasks.get(key)
        if ts is None:
            return {}

        if ts.state == "processing":
            retries = ts.retries
            if retries > 0:
                ts.retries = retries - 1
                recommendations = self.transition(key, "waiting")
            else:
                recommendations = self.transition(
                    key,
                    "erred",
                    cause=key,
                    exception=exception,
                    traceback=traceback,
                    worker=worker,
                    **kwargs
                )
        else:
            recommendations = {}

        return recommendations

    def stimulus_missing_data(
        self, cause=None, key=None, worker=None, ensure=True, **kwargs
    ):
        """ Mark that certain keys have gone missing.  Recover. """
        with log_errors():
            logger.debug("Stimulus missing data %s, %s", key, worker)

            ts = self.tasks.get(key)
            if ts is None or ts.state == "memory":
                return {}
            cts = self.tasks.get(cause)

            recommendations = OrderedDict()

            if cts is not None and cts.state == "memory":  # couldn't find this
                for ws in cts.who_has:  # TODO: this behavior is extreme
                    ws.has_what.remove(cts)
                    ws.nbytes -= cts.get_nbytes()
                cts.who_has.clear()
                recommendations[cause] = "released"

            if key:
                recommendations[key] = "released"

            self.transitions(recommendations)

            if self.validate:
                assert cause not in self.who_has

            return {}

    def stimulus_retry(self, comm=None, keys=None, client=None):
        logger.info("Client %s requests to retry %d keys", client, len(keys))
        if client:
            self.log_event(client, {"action": "retry", "count": len(keys)})

        stack = list(keys)
        seen = set()
        roots = []
        while stack:
            key = stack.pop()
            seen.add(key)
            erred_deps = [
                dts.key for dts in self.tasks[key].dependencies if dts.state == "erred"
            ]
            if erred_deps:
                stack.extend(erred_deps)
            else:
                roots.append(key)

        recommendations = {key: "waiting" for key in roots}
        self.transitions(recommendations)

        if self.validate:
            for key in seen:
                assert not self.tasks[key].exception_blame

        return tuple(seen)

    def remove_worker(self, comm=None, address=None, safe=False, close=True):
        """
        Remove worker from cluster

        We do this when a worker reports that it plans to leave or when it
        appears to be unresponsive.  This may send its tasks back to a released
        state.
        """
        with log_errors():
            if self.status == "closed":
                return

            address = self.coerce_address(address)

            if address not in self.workers:
                return "already-removed"

            host = get_address_host(address)

            ws = self.workers[address]

            self.log_event(
                ["all", address],
                {
                    "action": "remove-worker",
                    "worker": address,
                    "processing-tasks": dict(ws.processing),
                },
            )
            logger.info("Remove worker %s", ws)
            if close:
                with ignoring(AttributeError, CommClosedError):
                    self.stream_comms[address].send({"op": "close", "report": False})

            self.remove_resources(address)

            self.host_info[host]["nthreads"] -= ws.nthreads
            self.host_info[host]["addresses"].remove(address)
            self.total_nthreads -= ws.nthreads

            if not self.host_info[host]["addresses"]:
                del self.host_info[host]

            self.rpc.remove(address)
            del self.stream_comms[address]
            del self.aliases[ws.name]
            self.idle.discard(ws)
            self.saturated.discard(ws)
            del self.workers[address]
            ws.status = "closed"
            self.total_occupancy -= ws.occupancy

            recommendations = OrderedDict()

            for ts in list(ws.processing):
                k = ts.key
                recommendations[k] = "released"
                if not safe:
                    ts.suspicious += 1
                    ts.prefix.suspicious += 1
                    if ts.suspicious > self.allowed_failures:
                        del recommendations[k]
                        e = pickle.dumps(
                            KilledWorker(task=k, last_worker=ws.clean()), -1
                        )
                        r = self.transition(k, "erred", exception=e, cause=k)
                        recommendations.update(r)

            for ts in ws.has_what:
                ts.who_has.remove(ws)
                if not ts.who_has:
                    if ts.run_spec:
                        recommendations[ts.key] = "released"
                    else:  # pure data
                        recommendations[ts.key] = "forgotten"
            ws.has_what.clear()

            self.transitions(recommendations)

            for plugin in self.plugins[:]:
                try:
                    plugin.remove_worker(scheduler=self, worker=address)
                except Exception as e:
                    logger.exception(e)

            if not self.workers:
                logger.info("Lost all workers")

            for w in self.workers:
                self.bandwidth_workers.pop((address, w), None)
                self.bandwidth_workers.pop((w, address), None)

            def remove_worker_from_events():
                # If the worker isn't registered anymore after the delay, remove from events
                if address not in self.workers and address in self.events:
                    del self.events[address]

            cleanup_delay = parse_timedelta(
                dask.config.get("distributed.scheduler.events-cleanup-delay")
            )
            self.loop.call_later(cleanup_delay, remove_worker_from_events)
            logger.debug("Removed worker %s", ws)

        return "OK"

    def stimulus_cancel(self, comm, keys=None, client=None, force=False):
        """ Stop execution on a list of keys """
        logger.info("Client %s requests to cancel %d keys", client, len(keys))
        if client:
            self.log_event(
                client, {"action": "cancel", "count": len(keys), "force": force}
            )
        for key in keys:
            self.cancel_key(key, client, force=force)

    def cancel_key(self, key, client, retries=5, force=False):
        """ Cancel a particular key and all dependents """
        # TODO: this should be converted to use the transition mechanism
        ts = self.tasks.get(key)
        try:
            cs = self.clients[client]
        except KeyError:
            return
        if ts is None or not ts.who_wants:  # no key yet, lets try again in a moment
            if retries:
                self.loop.call_later(
                    0.2, lambda: self.cancel_key(key, client, retries - 1)
                )
            return
        if force or ts.who_wants == {cs}:  # no one else wants this key
            for dts in list(ts.dependents):
                self.cancel_key(dts.key, client, force=force)
        logger.info("Scheduler cancels key %s.  Force=%s", key, force)
        self.report({"op": "cancelled-key", "key": key})
        clients = list(ts.who_wants) if force else [cs]
        for c in clients:
            self.client_releases_keys(keys=[key], client=c.client_key)

    def client_desires_keys(self, keys=None, client=None):
        cs = self.clients.get(client)
        if cs is None:
            # For publish, queues etc.
            cs = self.clients[client] = ClientState(client)
        for k in keys:
            ts = self.tasks.get(k)
            if ts is None:
                # For publish, queues etc.
                ts = self.new_task(k, None, "released")
            ts.who_wants.add(cs)
            cs.wants_what.add(ts)

            if ts.state in ("memory", "erred"):
                self.report_on_key(k, client=client)

    def client_releases_keys(self, keys=None, client=None):
        """ Remove keys from client desired list """
        logger.debug("Client %s releases keys: %s", client, keys)
        cs = self.clients[client]
        tasks2 = set()
        for key in list(keys):
            ts = self.tasks.get(key)
            if ts is not None and ts in cs.wants_what:
                cs.wants_what.remove(ts)
                s = ts.who_wants
                s.remove(cs)
                if not s:
                    tasks2.add(ts)

        recommendations = {}
        for ts in tasks2:
            if not ts.dependents:
                # No live dependents, can forget
                recommendations[ts.key] = "forgotten"
            elif ts.state != "erred" and not ts.waiters:
                recommendations[ts.key] = "released"

        self.transitions(recommendations)

    def client_heartbeat(self, client=None):
        """ Handle heartbeats from Client """
        self.clients[client].last_seen = time()

    ###################
    # Task Validation #
    ###################

    def validate_released(self, key):
        ts = self.tasks[key]
        assert ts.state == "released"
        assert not ts.waiters
        assert not ts.waiting_on
        assert not ts.who_has
        assert not ts.processing_on
        assert not any(ts in dts.waiters for dts in ts.dependencies)
        assert ts not in self.unrunnable

    def validate_waiting(self, key):
        ts = self.tasks[key]
        assert ts.waiting_on
        assert not ts.who_has
        assert not ts.processing_on
        assert ts not in self.unrunnable
        for dts in ts.dependencies:
            # We are waiting on a dependency iff it's not stored
            assert bool(dts.who_has) + (dts in ts.waiting_on) == 1
            assert ts in dts.waiters  # XXX even if dts.who_has?

    def validate_processing(self, key):
        ts = self.tasks[key]
        assert not ts.waiting_on
        ws = ts.processing_on
        assert ws
        assert ts in ws.processing
        assert not ts.who_has
        for dts in ts.dependencies:
            assert dts.who_has
            assert ts in dts.waiters

    def validate_memory(self, key):
        ts = self.tasks[key]
        assert ts.who_has
        assert not ts.processing_on
        assert not ts.waiting_on
        assert ts not in self.unrunnable
        for dts in ts.dependents:
            assert (dts in ts.waiters) == (dts.state in ("waiting", "processing"))
            assert ts not in dts.waiting_on

    def validate_no_worker(self, key):
        ts = self.tasks[key]
        assert ts in self.unrunnable
        assert not ts.waiting_on
        assert ts in self.unrunnable
        assert not ts.processing_on
        assert not ts.who_has
        for dts in ts.dependencies:
            assert dts.who_has

    def validate_erred(self, key):
        ts = self.tasks[key]
        assert ts.exception_blame
        assert not ts.who_has

    def validate_key(self, key, ts=None):
        try:
            if ts is None:
                ts = self.tasks.get(key)
            if ts is None:
                logger.debug("Key lost: %s", key)
            else:
                ts.validate()
                try:
                    func = getattr(self, "validate_" + ts.state.replace("-", "_"))
                except AttributeError:
                    logger.error(
                        "self.validate_%s not found", ts.state.replace("-", "_")
                    )
                else:
                    func(key)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def validate_state(self, allow_overlap=False):
        validate_state(self.tasks, self.workers, self.clients)

        if not (set(self.workers) == set(self.stream_comms)):
            raise ValueError("Workers not the same in all collections")

        for w, ws in self.workers.items():
            assert isinstance(w, str), (type(w), w)
            assert isinstance(ws, WorkerState), (type(ws), ws)
            assert ws.address == w
            if not ws.processing:
                assert not ws.occupancy
                assert ws in self.idle

        for k, ts in self.tasks.items():
            assert isinstance(ts, TaskState), (type(ts), ts)
            assert ts.key == k
            self.validate_key(k, ts)

        for c, cs in self.clients.items():
            # client=None is often used in tests...
            assert c is None or isinstance(c, str), (type(c), c)
            assert isinstance(cs, ClientState), (type(cs), cs)
            assert cs.client_key == c

        a = {w: ws.nbytes for w, ws in self.workers.items()}
        b = {
            w: sum(ts.get_nbytes() for ts in ws.has_what)
            for w, ws in self.workers.items()
        }
        assert a == b, (a, b)

        actual_total_occupancy = 0
        for worker, ws in self.workers.items():
            assert abs(sum(ws.processing.values()) - ws.occupancy) < 1e-8
            actual_total_occupancy += ws.occupancy

        assert abs(actual_total_occupancy - self.total_occupancy) < 1e-8, (
            actual_total_occupancy,
            self.total_occupancy,
        )

    ###################
    # Manage Messages #
    ###################

    def report(self, msg, ts=None, client=None):
        """
        Publish updates to all listening Queues and Comms

        If the message contains a key then we only send the message to those
        comms that care about the key.
        """
        comms = set()
        if client is not None:
            try:
                comms.add(self.client_comms[client])
            except KeyError:
                pass

        if ts is None and "key" in msg:
            ts = self.tasks.get(msg["key"])
        if ts is None:
            # Notify all clients
            comms |= set(self.client_comms.values())
        else:
            # Notify clients interested in key
            comms |= {
                self.client_comms[c.client_key]
                for c in ts.who_wants
                if c.client_key in self.client_comms
            }
        for c in comms:
            try:
                c.send(msg)
                # logger.debug("Scheduler sends message to client %s", msg)
            except CommClosedError:
                if self.status == "running":
                    logger.critical("Tried writing to closed comm: %s", msg)

    async def add_client(self, comm, client=None, versions=None):
        """ Add client to network

        We listen to all future messages from this Comm.
        """
        assert client is not None
        comm.name = "Scheduler->Client"
        logger.info("Receive client connection: %s", client)
        self.log_event(["all", client], {"action": "add-client", "client": client})
        self.clients[client] = ClientState(client, versions=versions)

        for plugin in self.plugins[:]:
            try:
                plugin.add_client(scheduler=self, client=client)
            except Exception as e:
                logger.exception(e)

        try:
            bcomm = BatchedSend(interval="2ms", loop=self.loop)
            bcomm.start(comm)
            self.client_comms[client] = bcomm
            msg = {"op": "stream-start"}
            version_warning = version_module.error_message(
                version_module.get_versions(),
                {w: ws.versions for w, ws in self.workers.items()},
                versions,
            )
            if version_warning:
                msg["warning"] = version_warning
            bcomm.send(msg)

            try:
                await self.handle_stream(comm=comm, extra={"client": client})
            finally:
                self.remove_client(client=client)
                logger.debug("Finished handling client %s", client)
        finally:
            if not comm.closed():
                self.client_comms[client].send({"op": "stream-closed"})
            try:
                if not shutting_down():
                    await self.client_comms[client].close()
                    del self.client_comms[client]
                    if self.status == "running":
                        logger.info("Close client connection: %s", client)
            except TypeError:  # comm becomes None during GC
                pass

    def remove_client(self, client=None):
        """ Remove client from network """
        if self.status == "running":
            logger.info("Remove client %s", client)
        self.log_event(["all", client], {"action": "remove-client", "client": client})
        try:
            cs = self.clients[client]
        except KeyError:
            # XXX is this a legitimate condition?
            pass
        else:
            self.client_releases_keys(
                keys=[ts.key for ts in cs.wants_what], client=cs.client_key
            )
            del self.clients[client]

            for plugin in self.plugins[:]:
                try:
                    plugin.remove_client(scheduler=self, client=client)
                except Exception as e:
                    logger.exception(e)

        def remove_client_from_events():
            # If the client isn't registered anymore after the delay, remove from events
            if client not in self.clients and client in self.events:
                del self.events[client]

        cleanup_delay = parse_timedelta(
            dask.config.get("distributed.scheduler.events-cleanup-delay")
        )
        self.loop.call_later(cleanup_delay, remove_client_from_events)

    def send_task_to_worker(self, worker, key):
        """ Send a single computational task to a worker """
        try:
            ts = self.tasks[key]

            msg = {
                "op": "compute-task",
                "key": key,
                "priority": ts.priority,
                "duration": self.get_task_duration(ts),
            }
            if ts.resource_restrictions:
                msg["resource_restrictions"] = ts.resource_restrictions
            if ts.actor:
                msg["actor"] = True

            deps = ts.dependencies
            if deps:
                msg["who_has"] = {
                    dep.key: [ws.address for ws in dep.who_has] for dep in deps
                }
                msg["nbytes"] = {dep.key: dep.nbytes for dep in deps}

            if self.validate and deps:
                assert all(msg["who_has"].values())

            task = ts.run_spec
            if type(task) is dict:
                msg.update(task)
            else:
                msg["task"] = task

            self.worker_send(worker, msg)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def handle_uncaught_error(self, **msg):
        logger.exception(clean_exception(**msg)[1])

    def handle_task_finished(self, key=None, worker=None, **msg):
        if worker not in self.workers:
            return
        validate_key(key)
        r = self.stimulus_task_finished(key=key, worker=worker, **msg)
        self.transitions(r)

    def handle_task_erred(self, key=None, **msg):
        r = self.stimulus_task_erred(key=key, **msg)
        self.transitions(r)

    def handle_release_data(self, key=None, worker=None, client=None, **msg):
        ts = self.tasks.get(key)
        if ts is None:
            return
        ws = self.workers[worker]
        if ts.processing_on != ws:
            return
        r = self.stimulus_missing_data(key=key, ensure=False, **msg)
        self.transitions(r)

    def handle_missing_data(self, key=None, errant_worker=None, **kwargs):
        logger.debug("handle missing data key=%s worker=%s", key, errant_worker)
        self.log.append(("missing", key, errant_worker))

        ts = self.tasks.get(key)
        if ts is None or not ts.who_has:
            return
        if errant_worker in self.workers:
            ws = self.workers[errant_worker]
            if ws in ts.who_has:
                ts.who_has.remove(ws)
                ws.has_what.remove(ts)
                ws.nbytes -= ts.get_nbytes()
        if not ts.who_has:
            if ts.run_spec:
                self.transitions({key: "released"})
            else:
                self.transitions({key: "forgotten"})

    def release_worker_data(self, stream=None, keys=None, worker=None):
        ws = self.workers[worker]
        tasks = {self.tasks[k] for k in keys}
        removed_tasks = tasks & ws.has_what
        ws.has_what -= removed_tasks

        recommendations = {}
        for ts in removed_tasks:
            ws.nbytes -= ts.get_nbytes()
            wh = ts.who_has
            wh.remove(ws)
            if not wh:
                recommendations[ts.key] = "released"
        if recommendations:
            self.transitions(recommendations)

    def handle_long_running(self, key=None, worker=None, compute_duration=None):
        """ A task has seceded from the thread pool

        We stop the task from being stolen in the future, and change task
        duration accounting as if the task has stopped.
        """
        ts = self.tasks[key]
        if "stealing" in self.extensions:
            self.extensions["stealing"].remove_key_from_stealable(ts)

        ws = ts.processing_on
        if ws is None:
            logger.debug("Received long-running signal from duplicate task. Ignoring.")
            return

        if compute_duration:
            old_duration = ts.prefix.duration_average or 0
            new_duration = compute_duration
            if not old_duration:
                avg_duration = new_duration
            else:
                avg_duration = 0.5 * old_duration + 0.5 * new_duration

            ts.prefix.duration_average = avg_duration

        ws.occupancy -= ws.processing[ts]
        self.total_occupancy -= ws.processing[ts]
        ws.processing[ts] = 0
        self.check_idle_saturated(ws)

    async def handle_worker(self, comm=None, worker=None):
        """
        Listen to responses from a single worker

        This is the main loop for scheduler-worker interaction

        See Also
        --------
        Scheduler.handle_client: Equivalent coroutine for clients
        """
        comm.name = "Scheduler connection to worker"
        worker_comm = self.stream_comms[worker]
        worker_comm.start(comm)
        logger.info("Starting worker compute stream, %s", worker)
        try:
            await self.handle_stream(comm=comm, extra={"worker": worker})
        finally:
            if worker in self.stream_comms:
                worker_comm.abort()
                self.remove_worker(address=worker)

    def add_plugin(self, plugin=None, idempotent=False, **kwargs):
        """
        Add external plugin to scheduler

        See https://distributed.readthedocs.io/en/latest/plugins.html
        """
        if isinstance(plugin, type):
            plugin = plugin(self, **kwargs)

        if idempotent and any(isinstance(p, type(plugin)) for p in self.plugins):
            return

        self.plugins.append(plugin)

    def remove_plugin(self, plugin):
        """ Remove external plugin from scheduler """
        self.plugins.remove(plugin)

    def worker_send(self, worker, msg):
        """ Send message to worker

        This also handles connection failures by adding a callback to remove
        the worker on the next cycle.
        """
        try:
            self.stream_comms[worker].send(msg)
        except (CommClosedError, AttributeError):
            self.loop.add_callback(self.remove_worker, address=worker)

    ############################
    # Less common interactions #
    ############################

    async def scatter(
        self,
        comm=None,
        data=None,
        workers=None,
        client=None,
        broadcast=False,
        timeout=2,
    ):
        """ Send data out to workers

        See also
        --------
        Scheduler.broadcast:
        """
        start = time()
        while not self.workers:
            await asyncio.sleep(0.2)
            if time() > start + timeout:
                raise TimeoutError("No workers found")

        if workers is None:
            nthreads = {w: ws.nthreads for w, ws in self.workers.items()}
        else:
            workers = [self.coerce_address(w) for w in workers]
            nthreads = {w: self.workers[w].nthreads for w in workers}

        assert isinstance(data, dict)

        keys, who_has, nbytes = await scatter_to_workers(
            nthreads, data, rpc=self.rpc, report=False
        )

        self.update_data(who_has=who_has, nbytes=nbytes, client=client)

        if broadcast:
            if broadcast == True:  # noqa: E712
                n = len(nthreads)
            else:
                n = broadcast
            await self.replicate(keys=keys, workers=workers, n=n)

        self.log_event(
            [client, "all"], {"action": "scatter", "client": client, "count": len(data)}
        )
        return keys

    async def gather(self, comm=None, keys=None, serializers=None):
        """ Collect data in from workers """
        keys = list(keys)
        who_has = {}
        for key in keys:
            ts = self.tasks.get(key)
            if ts is not None:
                who_has[key] = [ws.address for ws in ts.who_has]
            else:
                who_has[key] = []

        data, missing_keys, missing_workers = await gather_from_workers(
            who_has, rpc=self.rpc, close=False, serializers=serializers
        )
        if not missing_keys:
            result = {"status": "OK", "data": data}
        else:
            missing_states = [
                (self.tasks[key].state if key in self.tasks else None)
                for key in missing_keys
            ]
            logger.exception(
                "Couldn't gather keys %s state: %s workers: %s",
                missing_keys,
                missing_states,
                missing_workers,
            )
            result = {"status": "error", "keys": missing_keys}
            with log_errors():
                # Remove suspicious workers from the scheduler but allow them to
                # reconnect.
                for worker in missing_workers:
                    self.remove_worker(address=worker, close=False)
                for key, workers in missing_keys.items():
                    # Task may already be gone if it was held by a
                    # `missing_worker`
                    ts = self.tasks.get(key)
                    logger.exception(
                        "Workers don't have promised key: %s, %s",
                        str(workers),
                        str(key),
                    )
                    if not workers or ts is None:
                        continue
                    for worker in workers:
                        ws = self.workers.get(worker)
                        if ws is not None and ts in ws.has_what:
                            ws.has_what.remove(ts)
                            ts.who_has.remove(ws)
                            ws.nbytes -= ts.get_nbytes()
                            self.transitions({key: "released"})

        self.log_event("all", {"action": "gather", "count": len(keys)})
        return result

    def clear_task_state(self):
        # XXX what about nested state such as ClientState.wants_what
        # (see also fire-and-forget...)
        logger.info("Clear task state")
        for collection in self._task_state_collections:
            collection.clear()

    async def restart(self, client=None, timeout=3):
        """ Restart all workers.  Reset local state. """
        with log_errors():

            n_workers = len(self.workers)

            logger.info("Send lost future signal to clients")
            for cs in self.clients.values():
                self.client_releases_keys(
                    keys=[ts.key for ts in cs.wants_what], client=cs.client_key
                )

            nannies = {addr: ws.nanny for addr, ws in self.workers.items()}

            for addr in list(self.workers):
                try:
                    # Ask the worker to close if it doesn't have a nanny,
                    # otherwise the nanny will kill it anyway
                    self.remove_worker(address=addr, close=addr not in nannies)
                except Exception as e:
                    logger.info(
                        "Exception while restarting.  This is normal", exc_info=True
                    )

            self.clear_task_state()

            for plugin in self.plugins[:]:
                try:
                    plugin.restart(self)
                except Exception as e:
                    logger.exception(e)

            logger.debug("Send kill signal to nannies: %s", nannies)

            nannies = [
                rpc(nanny_address, connection_args=self.connection_args)
                for nanny_address in nannies.values()
                if nanny_address is not None
            ]

            resps = All(
                [
                    nanny.restart(
                        close=True, timeout=timeout * 0.8, executor_wait=False
                    )
                    for nanny in nannies
                ]
            )
            try:
                resps = await asyncio.wait_for(resps, timeout)
            except TimeoutError:
                logger.error(
                    "Nannies didn't report back restarted within "
                    "timeout.  Continuuing with restart process"
                )
            else:
                if not all(resp == "OK" for resp in resps):
                    logger.error(
                        "Not all workers responded positively: %s", resps, exc_info=True
                    )
            finally:
                await asyncio.gather(*[nanny.close_rpc() for nanny in nannies])

            await self.start()

            self.log_event([client, "all"], {"action": "restart", "client": client})
            start = time()
            while time() < start + 10 and len(self.workers) < n_workers:
                await asyncio.sleep(0.01)

            self.report({"op": "restart"})

    async def broadcast(
        self,
        comm=None,
        msg=None,
        workers=None,
        hosts=None,
        nanny=False,
        serializers=None,
    ):
        """ Broadcast message to workers, return all results """
        if workers is None or workers is True:
            if hosts is None:
                workers = list(self.workers)
            else:
                workers = []
        if hosts is not None:
            for host in hosts:
                if host in self.host_info:
                    workers.extend(self.host_info[host]["addresses"])
        # TODO replace with worker_list

        if nanny:
            addresses = [self.workers[w].nanny for w in workers]
        else:
            addresses = workers

        async def send_message(addr):
            comm = await connect(
                addr, deserialize=self.deserialize, connection_args=self.connection_args
            )
            comm.name = "Scheduler Broadcast"
            resp = await send_recv(comm, close=True, serializers=serializers, **msg)
            return resp

        results = await All(
            [send_message(address) for address in addresses if address is not None]
        )

        return dict(zip(workers, results))

    async def proxy(self, comm=None, msg=None, worker=None, serializers=None):
        """ Proxy a communication through the scheduler to some other worker """
        d = await self.broadcast(
            comm=comm, msg=msg, workers=[worker], serializers=serializers
        )
        return d[worker]

    async def _delete_worker_data(self, worker_address, keys):
        """ Delete data from a worker and update the corresponding worker/task states

        Parameters
        ----------
        worker_address: str
            Worker address to delete keys from
        keys: List[str]
            List of keys to delete on the specified worker
        """
        await retry_operation(
            self.rpc(addr=worker_address).delete_data, keys=list(keys), report=False
        )

        ws = self.workers[worker_address]
        tasks = {self.tasks[key] for key in keys}
        ws.has_what -= tasks
        for ts in tasks:
            ts.who_has.remove(ws)
            ws.nbytes -= ts.get_nbytes()
        self.log_event(ws.address, {"action": "remove-worker-data", "keys": keys})

    async def rebalance(self, comm=None, keys=None, workers=None):
        """ Rebalance keys so that each worker stores roughly equal bytes

        **Policy**

        This orders the workers by what fraction of bytes of the existing keys
        they have.  It walks down this list from most-to-least.  At each worker
        it sends the largest results it can find and sends them to the least
        occupied worker until either the sender or the recipient are at the
        average expected load.
        """
        with log_errors():
            async with self._lock:
                if keys:
                    tasks = {self.tasks[k] for k in keys}
                    missing_data = [ts.key for ts in tasks if not ts.who_has]
                    if missing_data:
                        return {"status": "missing-data", "keys": missing_data}
                else:
                    tasks = set(self.tasks.values())

                if workers:
                    workers = {self.workers[w] for w in workers}
                    workers_by_task = {ts: ts.who_has & workers for ts in tasks}
                else:
                    workers = set(self.workers.values())
                    workers_by_task = {ts: ts.who_has for ts in tasks}

                tasks_by_worker = {ws: set() for ws in workers}

                for k, v in workers_by_task.items():
                    for vv in v:
                        tasks_by_worker[vv].add(k)

                worker_bytes = {
                    ws: sum(ts.get_nbytes() for ts in v)
                    for ws, v in tasks_by_worker.items()
                }

                avg = sum(worker_bytes.values()) / len(worker_bytes)

                sorted_workers = list(
                    map(first, sorted(worker_bytes.items(), key=second, reverse=True))
                )

                recipients = iter(reversed(sorted_workers))
                recipient = next(recipients)
                msgs = []  # (sender, recipient, key)
                for sender in sorted_workers[: len(workers) // 2]:
                    sender_keys = {
                        ts: ts.get_nbytes() for ts in tasks_by_worker[sender]
                    }
                    sender_keys = iter(
                        sorted(sender_keys.items(), key=second, reverse=True)
                    )

                    try:
                        while worker_bytes[sender] > avg:
                            while (
                                worker_bytes[recipient] < avg
                                and worker_bytes[sender] > avg
                            ):
                                ts, nb = next(sender_keys)
                                if ts not in tasks_by_worker[recipient]:
                                    tasks_by_worker[recipient].add(ts)
                                    # tasks_by_worker[sender].remove(ts)
                                    msgs.append((sender, recipient, ts))
                                    worker_bytes[sender] -= nb
                                    worker_bytes[recipient] += nb
                            if worker_bytes[sender] > avg:
                                recipient = next(recipients)
                    except StopIteration:
                        break

                to_recipients = defaultdict(lambda: defaultdict(list))
                to_senders = defaultdict(list)
                for sender, recipient, ts in msgs:
                    to_recipients[recipient.address][ts.key].append(sender.address)
                    to_senders[sender.address].append(ts.key)

                result = await asyncio.gather(
                    *(
                        retry_operation(self.rpc(addr=r).gather, who_has=v)
                        for r, v in to_recipients.items()
                    )
                )
                for r, v in to_recipients.items():
                    self.log_event(r, {"action": "rebalance", "who_has": v})

                self.log_event(
                    "all",
                    {
                        "action": "rebalance",
                        "total-keys": len(tasks),
                        "senders": valmap(len, to_senders),
                        "recipients": valmap(len, to_recipients),
                        "moved_keys": len(msgs),
                    },
                )

                if not all(r["status"] == "OK" for r in result):
                    return {
                        "status": "missing-data",
                        "keys": tuple(concat(r["keys"].keys() for r in result)),
                    }

                for sender, recipient, ts in msgs:
                    assert ts.state == "memory"
                    ts.who_has.add(recipient)
                    recipient.has_what.add(ts)
                    recipient.nbytes += ts.get_nbytes()
                    self.log.append(
                        ("rebalance", ts.key, time(), sender.address, recipient.address)
                    )

                await asyncio.gather(
                    *(self._delete_worker_data(r, v) for r, v in to_senders.items())
                )

                return {"status": "OK"}

    async def replicate(
        self,
        comm=None,
        keys=None,
        n=None,
        workers=None,
        branching_factor=2,
        delete=True,
        lock=True,
    ):
        """ Replicate data throughout cluster

        This performs a tree copy of the data throughout the network
        individually on each piece of data.

        Parameters
        ----------
        keys: Iterable
            list of keys to replicate
        n: int
            Number of replications we expect to see within the cluster
        branching_factor: int, optional
            The number of workers that can copy data in each generation.
            The larger the branching factor, the more data we copy in
            a single step, but the more a given worker risks being
            swamped by data requests.

        See also
        --------
        Scheduler.rebalance
        """
        assert branching_factor > 0
        async with self._lock if lock else empty_context:
            workers = {self.workers[w] for w in self.workers_list(workers)}
            if n is None:
                n = len(workers)
            else:
                n = min(n, len(workers))
            if n == 0:
                raise ValueError("Can not use replicate to delete data")

            tasks = {self.tasks[k] for k in keys}
            missing_data = [ts.key for ts in tasks if not ts.who_has]
            if missing_data:
                return {"status": "missing-data", "keys": missing_data}

            # Delete extraneous data
            if delete:
                del_worker_tasks = defaultdict(set)
                for ts in tasks:
                    del_candidates = ts.who_has & workers
                    if len(del_candidates) > n:
                        for ws in random.sample(
                            del_candidates, len(del_candidates) - n
                        ):
                            del_worker_tasks[ws].add(ts)

                await asyncio.gather(
                    *(
                        self._delete_worker_data(ws.address, [t.key for t in tasks])
                        for ws, tasks in del_worker_tasks.items()
                    )
                )

            # Copy not-yet-filled data
            while tasks:
                gathers = defaultdict(dict)
                for ts in list(tasks):
                    n_missing = n - len(ts.who_has & workers)
                    if n_missing <= 0:
                        # Already replicated enough
                        tasks.remove(ts)
                        continue

                    count = min(n_missing, branching_factor * len(ts.who_has))
                    assert count > 0

                    for ws in random.sample(workers - ts.who_has, count):
                        gathers[ws.address][ts.key] = [
                            wws.address for wws in ts.who_has
                        ]

                results = await asyncio.gather(
                    *(
                        retry_operation(self.rpc(addr=w).gather, who_has=who_has)
                        for w, who_has in gathers.items()
                    )
                )
                for w, v in zip(gathers, results):
                    if v["status"] == "OK":
                        self.add_keys(worker=w, keys=list(gathers[w]))
                    else:
                        logger.warning("Communication failed during replication: %s", v)

                    self.log_event(w, {"action": "replicate-add", "keys": gathers[w]})

            self.log_event(
                "all",
                {
                    "action": "replicate",
                    "workers": list(workers),
                    "key-count": len(keys),
                    "branching-factor": branching_factor,
                },
            )

    def workers_to_close(
        self,
        comm=None,
        memory_ratio=None,
        n=None,
        key=None,
        minimum=None,
        target=None,
        attribute="address",
    ):
        """
        Find workers that we can close with low cost

        This returns a list of workers that are good candidates to retire.
        These workers are not running anything and are storing
        relatively little data relative to their peers.  If all workers are
        idle then we still maintain enough workers to have enough RAM to store
        our data, with a comfortable buffer.

        This is for use with systems like ``distributed.deploy.adaptive``.

        Parameters
        ----------
        memory_factor: Number
            Amount of extra space we want to have for our stored data.
            Defaults two 2, or that we want to have twice as much memory as we
            currently have data.
        n: int
            Number of workers to close
        minimum: int
            Minimum number of workers to keep around
        key: Callable(WorkerState)
            An optional callable mapping a WorkerState object to a group
            affiliation.  Groups will be closed together.  This is useful when
            closing workers must be done collectively, such as by hostname.
        target: int
            Target number of workers to have after we close
        attribute : str
            The attribute of the WorkerState object to return, like "address"
            or "name".  Defaults to "address".

        Examples
        --------
        >>> scheduler.workers_to_close()
        ['tcp://192.168.0.1:1234', 'tcp://192.168.0.2:1234']

        Group workers by hostname prior to closing

        >>> scheduler.workers_to_close(key=lambda ws: ws.host)
        ['tcp://192.168.0.1:1234', 'tcp://192.168.0.1:4567']

        Remove two workers

        >>> scheduler.workers_to_close(n=2)

        Keep enough workers to have twice as much memory as we we need.

        >>> scheduler.workers_to_close(memory_ratio=2)

        Returns
        -------
        to_close: list of worker addresses that are OK to close

        See Also
        --------
        Scheduler.retire_workers
        """
        if target is not None and n is None:
            n = len(self.workers) - target
        if n is not None:
            if n < 0:
                n = 0
            target = len(self.workers) - n

        if n is None and memory_ratio is None:
            memory_ratio = 2

        with log_errors():
            if not n and all(ws.processing for ws in self.workers.values()):
                return []

            if key is None:
                key = lambda ws: ws.address
            if isinstance(key, bytes) and dask.config.get(
                "distributed.scheduler.pickle"
            ):
                key = pickle.loads(key)

            groups = groupby(key, self.workers.values())

            limit_bytes = {
                k: sum(ws.memory_limit for ws in v) for k, v in groups.items()
            }
            group_bytes = {k: sum(ws.nbytes for ws in v) for k, v in groups.items()}

            limit = sum(limit_bytes.values())
            total = sum(group_bytes.values())

            def _key(group):
                is_idle = not any(ws.processing for ws in groups[group])
                bytes = -group_bytes[group]
                return (is_idle, bytes)

            idle = sorted(groups, key=_key)

            to_close = []
            n_remain = len(self.workers)

            while idle:
                group = idle.pop()
                if n is None and any(ws.processing for ws in groups[group]):
                    break

                if minimum and n_remain - len(groups[group]) < minimum:
                    break

                limit -= limit_bytes[group]

                if (n is not None and n_remain - len(groups[group]) >= target) or (
                    memory_ratio is not None and limit >= memory_ratio * total
                ):
                    to_close.append(group)
                    n_remain -= len(groups[group])

                else:
                    break

            result = [getattr(ws, attribute) for g in to_close for ws in groups[g]]
            if result:
                logger.debug("Suggest closing workers: %s", result)

            return result

    async def retire_workers(
        self,
        comm=None,
        workers=None,
        remove=True,
        close_workers=False,
        names=None,
        lock=True,
        **kwargs
    ):
        """ Gracefully retire workers from cluster

        Parameters
        ----------
        workers: list (optional)
            List of worker addresses to retire.
            If not provided we call ``workers_to_close`` which finds a good set
        workers_names: list (optional)
            List of worker names to retire.
        remove: bool (defaults to True)
            Whether or not to remove the worker metadata immediately or else
            wait for the worker to contact us
        close_workers: bool (defaults to False)
            Whether or not to actually close the worker explicitly from here.
            Otherwise we expect some external job scheduler to finish off the
            worker.
        **kwargs: dict
            Extra options to pass to workers_to_close to determine which
            workers we should drop

        Returns
        -------
        Dictionary mapping worker ID/address to dictionary of information about
        that worker for each retired worker.

        See Also
        --------
        Scheduler.workers_to_close
        """
        with log_errors():
            async with self._lock if lock else empty_context:
                if names is not None:
                    if names:
                        logger.info("Retire worker names %s", names)
                    names = set(map(str, names))
                    workers = [
                        ws.address
                        for ws in self.workers.values()
                        if str(ws.name) in names
                    ]
                if workers is None:
                    while True:
                        try:
                            workers = self.workers_to_close(**kwargs)
                            if workers:
                                workers = await self.retire_workers(
                                    workers=workers,
                                    remove=remove,
                                    close_workers=close_workers,
                                    lock=False,
                                )
                            return workers
                        except KeyError:  # keys left during replicate
                            pass
                workers = {self.workers[w] for w in workers if w in self.workers}
                if not workers:
                    return []
                logger.info("Retire workers %s", workers)

                # Keys orphaned by retiring those workers
                keys = set.union(*[w.has_what for w in workers])
                keys = {ts.key for ts in keys if ts.who_has.issubset(workers)}

                other_workers = set(self.workers.values()) - workers
                if keys:
                    if other_workers:
                        logger.info("Moving %d keys to other workers", len(keys))
                        await self.replicate(
                            keys=keys,
                            workers=[ws.address for ws in other_workers],
                            n=1,
                            delete=False,
                            lock=False,
                        )
                    else:
                        return []

                worker_keys = {ws.address: ws.identity() for ws in workers}
                if close_workers and worker_keys:
                    await asyncio.gather(
                        *[self.close_worker(worker=w, safe=True) for w in worker_keys]
                    )
                if remove:
                    for w in worker_keys:
                        self.remove_worker(address=w, safe=True)

                self.log_event(
                    "all",
                    {
                        "action": "retire-workers",
                        "workers": worker_keys,
                        "moved-keys": len(keys),
                    },
                )
                self.log_event(list(worker_keys), {"action": "retired"})

                return worker_keys

    def add_keys(self, comm=None, worker=None, keys=()):
        """
        Learn that a worker has certain keys

        This should not be used in practice and is mostly here for legacy
        reasons.  However, it is sent by workers from time to time.
        """
        if worker not in self.workers:
            return "not found"
        ws = self.workers[worker]
        for key in keys:
            ts = self.tasks.get(key)
            if ts is not None and ts.state == "memory":
                if ts not in ws.has_what:
                    ws.nbytes += ts.get_nbytes()
                    ws.has_what.add(ts)
                    ts.who_has.add(ws)
            else:
                self.worker_send(
                    worker, {"op": "delete-data", "keys": [key], "report": False}
                )

        return "OK"

    @annotate("update_data", domain="distributed")
    def update_data(
        self, comm=None, who_has=None, nbytes=None, client=None, serializers=None
    ):
        """
        Learn that new data has entered the network from an external source

        See Also
        --------
        Scheduler.mark_key_in_memory
        """
        with log_errors():
            who_has = {
                k: [self.coerce_address(vv) for vv in v] for k, v in who_has.items()
            }
            logger.debug("Update data %s", who_has)

            for key, workers in who_has.items():
                ts = self.tasks.get(key)
                if ts is None:
                    ts = self.new_task(key, None, "memory")
                ts.state = "memory"
                if key in nbytes:
                    ts.set_nbytes(nbytes[key])
                for w in workers:
                    ws = self.workers[w]
                    if ts not in ws.has_what:
                        ws.nbytes += ts.get_nbytes()
                        ws.has_what.add(ts)
                        ts.who_has.add(ws)
                self.report(
                    {"op": "key-in-memory", "key": key, "workers": list(workers)}
                )

            if client:
                self.client_desires_keys(keys=list(who_has), client=client)

    def report_on_key(self, key=None, ts=None, client=None):
        assert (key is None) + (ts is None) == 1, (key, ts)
        if ts is None:
            try:
                ts = self.tasks[key]
            except KeyError:
                self.report({"op": "cancelled-key", "key": key}, client=client)
                return
        else:
            key = ts.key
        if ts.state == "forgotten":
            self.report({"op": "cancelled-key", "key": key}, ts=ts, client=client)
        elif ts.state == "memory":
            self.report({"op": "key-in-memory", "key": key}, ts=ts, client=client)
        elif ts.state == "erred":
            failing_ts = ts.exception_blame
            self.report(
                {
                    "op": "task-erred",
                    "key": key,
                    "exception": failing_ts.exception,
                    "traceback": failing_ts.traceback,
                },
                ts=ts,
                client=client,
            )

    async def feed(
        self, comm, function=None, setup=None, teardown=None, interval="1s", **kwargs
    ):
        """
        Provides a data Comm to external requester

        Caution: this runs arbitrary Python code on the scheduler.  This should
        eventually be phased out.  It is mostly used by diagnostics.
        """
        if not dask.config.get("distributed.scheduler.pickle"):
            logger.warn(
                "Tried to call 'feed' route with custom fucntions, but "
                "pickle is disallowed.  Set the 'distributed.scheduler.pickle'"
                "config value to True to use the 'feed' route (this is mostly "
                "commonly used with progress bars)"
            )
            return
        import pickle

        interval = parse_timedelta(interval)
        with log_errors():
            if function:
                function = pickle.loads(function)
            if setup:
                setup = pickle.loads(setup)
            if teardown:
                teardown = pickle.loads(teardown)
            state = setup(self) if setup else None
            if inspect.isawaitable(state):
                state = await state
            try:
                while self.status == "running":
                    if state is None:
                        response = function(self)
                    else:
                        response = function(self, state)
                    await comm.write(response)
                    await asyncio.sleep(interval)
            except (EnvironmentError, CommClosedError):
                pass
            finally:
                if teardown:
                    teardown(self, state)

    def subscribe_worker_status(self, comm=None):
        WorkerStatusPlugin(self, comm)
        ident = self.identity()
        for v in ident["workers"].values():
            del v["metrics"]
            del v["last_seen"]
        return ident

    def get_processing(self, comm=None, workers=None):
        if workers is not None:
            workers = set(map(self.coerce_address, workers))
            return {w: [ts.key for ts in self.workers[w].processing] for w in workers}
        else:
            return {
                w: [ts.key for ts in ws.processing] for w, ws in self.workers.items()
            }

    def get_who_has(self, comm=None, keys=None):
        if keys is not None:
            return {
                k: [ws.address for ws in self.tasks[k].who_has]
                if k in self.tasks
                else []
                for k in keys
            }
        else:
            return {
                key: [ws.address for ws in ts.who_has] for key, ts in self.tasks.items()
            }

    def get_has_what(self, comm=None, workers=None):
        if workers is not None:
            workers = map(self.coerce_address, workers)
            return {
                w: [ts.key for ts in self.workers[w].has_what]
                if w in self.workers
                else []
                for w in workers
            }
        else:
            return {w: [ts.key for ts in ws.has_what] for w, ws in self.workers.items()}

    def get_ncores(self, comm=None, workers=None):
        if workers is not None:
            workers = map(self.coerce_address, workers)
            return {w: self.workers[w].nthreads for w in workers if w in self.workers}
        else:
            return {w: ws.nthreads for w, ws in self.workers.items()}

    async def get_call_stack(self, comm=None, keys=None):
        if keys is not None:
            stack = list(keys)
            processing = set()
            while stack:
                key = stack.pop()
                ts = self.tasks[key]
                if ts.state == "waiting":
                    stack.extend(dts.key for dts in ts.dependencies)
                elif ts.state == "processing":
                    processing.add(ts)

            workers = defaultdict(list)
            for ts in processing:
                if ts.processing_on:
                    workers[ts.processing_on.address].append(ts.key)
        else:
            workers = {w: None for w in self.workers}

        if not workers:
            return {}

        results = await asyncio.gather(
            *(self.rpc(w).call_stack(keys=v) for w, v in workers.items())
        )
        response = {w: r for w, r in zip(workers, results) if r}
        return response

    def get_nbytes(self, comm=None, keys=None, summary=True):
        with log_errors():
            if keys is not None:
                result = {k: self.tasks[k].nbytes for k in keys}
            else:
                result = {
                    k: ts.nbytes
                    for k, ts in self.tasks.items()
                    if ts.nbytes is not None
                }

            if summary:
                out = defaultdict(lambda: 0)
                for k, v in result.items():
                    out[key_split(k)] += v
                result = dict(out)

            return result

    def get_comm_cost(self, ts, ws):
        """
        Get the estimated communication cost (in s.) to compute the task
        on the given worker.
        """
        return sum(dts.nbytes for dts in ts.dependencies - ws.has_what) / self.bandwidth

    def get_task_duration(self, ts, default=None):
        """
        Get the estimated computation cost of the given task
        (not including any communication cost).
        """
        duration = ts.prefix.duration_average
        if duration is None:
            self.unknown_durations[ts.prefix.name].add(ts)
            if default is None:
                default = parse_timedelta(
                    dask.config.get("distributed.scheduler.unknown-task-duration")
                )
            return default

        return duration

    def run_function(self, stream, function, args=(), kwargs={}, wait=True):
        """ Run a function within this process

        See Also
        --------
        Client.run_on_scheduler:
        """
        from .worker import run

        self.log_event("all", {"action": "run-function", "function": function})
        return run(self, stream, function=function, args=args, kwargs=kwargs, wait=wait)

    def set_metadata(self, stream=None, keys=None, value=None):
        try:
            metadata = self.task_metadata
            for key in keys[:-1]:
                if key not in metadata or not isinstance(metadata[key], (dict, list)):
                    metadata[key] = dict()
                metadata = metadata[key]
            metadata[keys[-1]] = value
        except Exception as e:
            import pdb

            pdb.set_trace()

    def get_metadata(self, stream=None, keys=None, default=no_default):
        metadata = self.task_metadata
        for key in keys[:-1]:
            metadata = metadata[key]
        try:
            return metadata[keys[-1]]
        except KeyError:
            if default != no_default:
                return default
            else:
                raise

    def get_task_status(self, stream=None, keys=None):
        return {
            key: (self.tasks[key].state if key in self.tasks else None) for key in keys
        }

    def get_task_stream(self, comm=None, start=None, stop=None, count=None):
        from distributed.diagnostics.task_stream import TaskStreamPlugin

        self.add_plugin(TaskStreamPlugin, idempotent=True)
        ts = [p for p in self.plugins if isinstance(p, TaskStreamPlugin)][0]
        return ts.collect(start=start, stop=stop, count=count)

    async def register_worker_plugin(self, comm, plugin, name=None):
        """ Registers a setup function, and call it on every worker """
        self.worker_plugins.append(plugin)

        responses = await self.broadcast(
            msg=dict(op="plugin-add", plugin=plugin, name=name)
        )
        return responses

    #####################
    # State Transitions #
    #####################

    def _remove_from_processing(self, ts, send_worker_msg=None):
        """
        Remove *ts* from the set of processing tasks.
        """
        ws = ts.processing_on
        ts.processing_on = None
        w = ws.address
        if w in self.workers:  # may have been removed
            duration = ws.processing.pop(ts)
            if not ws.processing:
                self.total_occupancy -= ws.occupancy
                ws.occupancy = 0
            else:
                self.total_occupancy -= duration
                ws.occupancy -= duration
            self.check_idle_saturated(ws)
            self.release_resources(ts, ws)
            if send_worker_msg:
                self.worker_send(w, send_worker_msg)

    def _add_to_memory(
        self, ts, ws, recommendations, type=None, typename=None, **kwargs
    ):
        """
        Add *ts* to the set of in-memory tasks.
        """
        if self.validate:
            assert ts not in ws.has_what

        ts.who_has.add(ws)
        ws.has_what.add(ts)
        ws.nbytes += ts.get_nbytes()

        deps = ts.dependents
        if len(deps) > 1:
            deps = sorted(deps, key=operator.attrgetter("priority"), reverse=True)
        for dts in deps:
            s = dts.waiting_on
            if ts in s:
                s.discard(ts)
                if not s:  # new task ready to run
                    recommendations[dts.key] = "processing"

        for dts in ts.dependencies:
            s = dts.waiters
            s.discard(ts)
            if not s and not dts.who_wants:
                recommendations[dts.key] = "released"

        if not ts.waiters and not ts.who_wants:
            recommendations[ts.key] = "released"
        else:
            msg = {"op": "key-in-memory", "key": ts.key}
            if type is not None:
                msg["type"] = type
            self.report(msg)

        ts.state = "memory"
        ts.type = typename
        ts.group.types.add(typename)

        cs = self.clients["fire-and-forget"]
        if ts in cs.wants_what:
            self.client_releases_keys(client="fire-and-forget", keys=[ts.key])

    def transition_released_waiting(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert ts.run_spec
                assert not ts.waiting_on
                assert not ts.who_has
                assert not ts.processing_on
                assert not any(dts.state == "forgotten" for dts in ts.dependencies)

            if ts.has_lost_dependencies:
                return {key: "forgotten"}

            ts.state = "waiting"

            recommendations = OrderedDict()

            for dts in ts.dependencies:
                if dts.exception_blame:
                    ts.exception_blame = dts.exception_blame
                    recommendations[key] = "erred"
                    return recommendations

            for dts in ts.dependencies:
                dep = dts.key
                if not dts.who_has:
                    ts.waiting_on.add(dts)
                if dts.state == "released":
                    recommendations[dep] = "waiting"
                else:
                    dts.waiters.add(ts)

            ts.waiters = {dts for dts in ts.dependents if dts.state == "waiting"}

            if not ts.waiting_on:
                if self.workers:
                    recommendations[key] = "processing"
                else:
                    self.unrunnable.add(ts)
                    ts.state = "no-worker"

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_no_worker_waiting(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert ts in self.unrunnable
                assert not ts.waiting_on
                assert not ts.who_has
                assert not ts.processing_on

            self.unrunnable.remove(ts)

            if ts.has_lost_dependencies:
                return {key: "forgotten"}

            recommendations = OrderedDict()

            for dts in ts.dependencies:
                dep = dts.key
                if not dts.who_has:
                    ts.waiting_on.add(dts)
                if dts.state == "released":
                    recommendations[dep] = "waiting"
                else:
                    dts.waiters.add(ts)

            ts.state = "waiting"

            if not ts.waiting_on:
                if self.workers:
                    recommendations[key] = "processing"
                else:
                    self.unrunnable.add(ts)
                    ts.state = "no-worker"

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def decide_worker(self, ts):
        """
        Decide on a worker for task *ts*.  Return a WorkerState.
        """
        valid_workers = self.valid_workers(ts)

        if not valid_workers and not ts.loose_restrictions and self.workers:
            self.unrunnable.add(ts)
            ts.state = "no-worker"
            return None

        if ts.dependencies or valid_workers is not True:
            worker = decide_worker(
                ts,
                self.workers.values(),
                valid_workers,
                partial(self.worker_objective, ts),
            )
        elif self.idle:
            if len(self.idle) < 20:  # smart but linear in small case
                worker = min(self.idle, key=operator.attrgetter("occupancy"))
            else:  # dumb but fast in large case
                worker = self.idle[self.n_tasks % len(self.idle)]
        else:
            if len(self.workers) < 20:  # smart but linear in small case
                worker = min(
                    self.workers.values(), key=operator.attrgetter("occupancy")
                )
            else:  # dumb but fast in large case
                worker = self.workers.values()[self.n_tasks % len(self.workers)]

        if self.validate:
            assert worker is None or isinstance(worker, WorkerState), (
                type(worker),
                worker,
            )
            assert worker.address in self.workers

        return worker

    def transition_waiting_processing(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert not ts.waiting_on
                assert not ts.who_has
                assert not ts.exception_blame
                assert not ts.processing_on
                assert not ts.has_lost_dependencies
                assert ts not in self.unrunnable
                assert all(dts.who_has for dts in ts.dependencies)

            ws = self.decide_worker(ts)
            if ws is None:
                return {}
            worker = ws.address

            duration = self.get_task_duration(ts)
            comm = self.get_comm_cost(ts, ws)

            ws.processing[ts] = duration + comm
            ts.processing_on = ws
            ws.occupancy += duration + comm
            self.total_occupancy += duration + comm
            ts.state = "processing"
            self.consume_resources(ts, ws)
            self.check_idle_saturated(ws)
            self.n_tasks += 1

            if ts.actor:
                ws.actors.add(ts)

            # logger.debug("Send job to worker: %s, %s", worker, key)

            self.send_task_to_worker(worker, key)

            return {}
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_waiting_memory(self, key, nbytes=None, worker=None, **kwargs):
        try:
            ws = self.workers[worker]
            ts = self.tasks[key]

            if self.validate:
                assert not ts.processing_on
                assert ts.waiting_on
                assert ts.state == "waiting"

            ts.waiting_on.clear()

            if nbytes is not None:
                ts.set_nbytes(nbytes)

            self.check_idle_saturated(ws)

            recommendations = OrderedDict()

            self._add_to_memory(ts, ws, recommendations, **kwargs)

            if self.validate:
                assert not ts.processing_on
                assert not ts.waiting_on
                assert ts.who_has

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_processing_memory(
        self,
        key,
        nbytes=None,
        type=None,
        typename=None,
        worker=None,
        startstops=None,
        **kwargs
    ):
        try:
            ts = self.tasks[key]
            assert worker
            assert isinstance(worker, str)

            if self.validate:
                assert ts.processing_on
                ws = ts.processing_on
                assert ts in ws.processing
                assert not ts.waiting_on
                assert not ts.who_has, (ts, ts.who_has)
                assert not ts.exception_blame
                assert ts.state == "processing"

            ws = self.workers.get(worker)
            if ws is None:
                return {key: "released"}

            if ws != ts.processing_on:  # someone else has this task
                logger.info(
                    "Unexpected worker completed task, likely due to"
                    " work stealing.  Expected: %s, Got: %s, Key: %s",
                    ts.processing_on,
                    ws,
                    key,
                )
                return {}

            if startstops:
                L = [
                    (startstop["start"], startstop["stop"])
                    for startstop in startstops
                    if startstop["action"] == "compute"
                ]
                if L:
                    compute_start, compute_stop = L[0]
                else:  # This is very rare
                    compute_start = compute_stop = None
            else:
                compute_start = compute_stop = None

            #############################
            # Update Timing Information #
            #############################
            if compute_start and ws.processing.get(ts, True):
                # Update average task duration for worker
                old_duration = ts.prefix.duration_average or 0
                new_duration = compute_stop - compute_start
                if not old_duration:
                    avg_duration = new_duration
                else:
                    avg_duration = 0.5 * old_duration + 0.5 * new_duration

                ts.prefix.duration_average = avg_duration
                ts.group.duration += new_duration

                for tts in self.unknown_durations.pop(ts.prefix.name, ()):
                    if tts.processing_on:
                        wws = tts.processing_on
                        old = wws.processing[tts]
                        comm = self.get_comm_cost(tts, wws)
                        wws.processing[tts] = avg_duration + comm
                        wws.occupancy += avg_duration + comm - old
                        self.total_occupancy += avg_duration + comm - old

            ############################
            # Update State Information #
            ############################
            if nbytes is not None:
                ts.set_nbytes(nbytes)

            recommendations = OrderedDict()

            self._remove_from_processing(ts)

            self._add_to_memory(ts, ws, recommendations, type=type, typename=typename)

            if self.validate:
                assert not ts.processing_on
                assert not ts.waiting_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_memory_released(self, key, safe=False):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert not ts.waiting_on
                assert not ts.processing_on
                if safe:
                    assert not ts.waiters

            if ts.actor:
                for ws in ts.who_has:
                    ws.actors.discard(ts)
                if ts.who_wants:
                    ts.exception_blame = ts
                    ts.exception = "Worker holding Actor was lost"
                    return {ts.key: "erred"}  # don't try to recreate

            recommendations = OrderedDict()

            for dts in ts.waiters:
                if dts.state in ("no-worker", "processing"):
                    recommendations[dts.key] = "waiting"
                elif dts.state == "waiting":
                    dts.waiting_on.add(ts)

            # XXX factor this out?
            for ws in ts.who_has:
                ws.has_what.remove(ts)
                ws.nbytes -= ts.get_nbytes()
                ts.group.nbytes_in_memory -= ts.get_nbytes()
                self.worker_send(
                    ws.address, {"op": "delete-data", "keys": [key], "report": False}
                )
            ts.who_has.clear()

            ts.state = "released"

            self.report({"op": "lost-data", "key": key})

            if not ts.run_spec:  # pure data
                recommendations[key] = "forgotten"
            elif ts.has_lost_dependencies:
                recommendations[key] = "forgotten"
            elif ts.who_wants or ts.waiters:
                recommendations[key] = "waiting"

            if self.validate:
                assert not ts.waiting_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_released_erred(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                with log_errors(pdb=LOG_PDB):
                    assert ts.exception_blame
                    assert not ts.who_has
                    assert not ts.waiting_on
                    assert not ts.waiters

            recommendations = {}

            failing_ts = ts.exception_blame

            for dts in ts.dependents:
                dts.exception_blame = failing_ts
                if not dts.who_has:
                    recommendations[dts.key] = "erred"

            self.report(
                {
                    "op": "task-erred",
                    "key": key,
                    "exception": failing_ts.exception,
                    "traceback": failing_ts.traceback,
                }
            )

            ts.state = "erred"

            # TODO: waiting data?
            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_erred_released(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                with log_errors(pdb=LOG_PDB):
                    assert all(dts.state != "erred" for dts in ts.dependencies)
                    assert ts.exception_blame
                    assert not ts.who_has
                    assert not ts.waiting_on
                    assert not ts.waiters

            recommendations = OrderedDict()

            ts.exception = None
            ts.exception_blame = None
            ts.traceback = None

            for dep in ts.dependents:
                if dep.state == "erred":
                    recommendations[dep.key] = "waiting"

            self.report({"op": "task-retried", "key": key})
            ts.state = "released"

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_waiting_released(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert not ts.who_has
                assert not ts.processing_on

            recommendations = {}

            for dts in ts.dependencies:
                s = dts.waiters
                if ts in s:
                    s.discard(ts)
                    if not s and not dts.who_wants:
                        recommendations[dts.key] = "released"
            ts.waiting_on.clear()

            ts.state = "released"

            if ts.has_lost_dependencies:
                recommendations[key] = "forgotten"
            elif not ts.exception_blame and (ts.who_wants or ts.waiters):
                recommendations[key] = "waiting"
            else:
                ts.waiters.clear()

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_processing_released(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert ts.processing_on
                assert not ts.who_has
                assert not ts.waiting_on
                assert self.tasks[key].state == "processing"

            self._remove_from_processing(
                ts, send_worker_msg={"op": "release-task", "key": key}
            )

            ts.state = "released"

            recommendations = OrderedDict()

            if ts.has_lost_dependencies:
                recommendations[key] = "forgotten"
            elif ts.waiters or ts.who_wants:
                recommendations[key] = "waiting"

            if recommendations.get(key) != "waiting":
                for dts in ts.dependencies:
                    if dts.state != "released":
                        s = dts.waiters
                        s.discard(ts)
                        if not s and not dts.who_wants:
                            recommendations[dts.key] = "released"
                ts.waiters.clear()

            if self.validate:
                assert not ts.processing_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_processing_erred(
        self, key, cause=None, exception=None, traceback=None, **kwargs
    ):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert cause or ts.exception_blame
                assert ts.processing_on
                assert not ts.who_has
                assert not ts.waiting_on

            if ts.actor:
                ws = ts.processing_on
                ws.actors.remove(ts)

            self._remove_from_processing(ts)

            if exception is not None:
                ts.exception = exception
            if traceback is not None:
                ts.traceback = traceback
            if cause is not None:
                failing_ts = self.tasks[cause]
                ts.exception_blame = failing_ts
            else:
                failing_ts = ts.exception_blame

            recommendations = {}

            for dts in ts.dependents:
                dts.exception_blame = failing_ts
                recommendations[dts.key] = "erred"

            for dts in ts.dependencies:
                s = dts.waiters
                s.discard(ts)
                if not s and not dts.who_wants:
                    recommendations[dts.key] = "released"

            ts.waiters.clear()  # do anything with this?

            ts.state = "erred"

            self.report(
                {
                    "op": "task-erred",
                    "key": key,
                    "exception": failing_ts.exception,
                    "traceback": failing_ts.traceback,
                }
            )

            cs = self.clients["fire-and-forget"]
            if ts in cs.wants_what:
                self.client_releases_keys(client="fire-and-forget", keys=[key])

            if self.validate:
                assert not ts.processing_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_no_worker_released(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert self.tasks[key].state == "no-worker"
                assert not ts.who_has
                assert not ts.waiting_on

            self.unrunnable.remove(ts)
            ts.state = "released"

            for dts in ts.dependencies:
                dts.waiters.discard(ts)

            ts.waiters.clear()

            return {}
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def remove_key(self, key):
        ts = self.tasks.pop(key)
        assert ts.state == "forgotten"
        self.unrunnable.discard(ts)
        for cs in ts.who_wants:
            cs.wants_what.remove(ts)
        ts.who_wants.clear()
        ts.processing_on = None
        ts.exception_blame = ts.exception = ts.traceback = None

        if key in self.task_metadata:
            del self.task_metadata[key]

    def _propagate_forgotten(self, ts, recommendations):
        ts.state = "forgotten"
        key = ts.key
        for dts in ts.dependents:
            dts.has_lost_dependencies = True
            dts.dependencies.remove(ts)
            dts.waiting_on.discard(ts)
            if dts.state not in ("memory", "erred"):
                # Cannot compute task anymore
                recommendations[dts.key] = "forgotten"
        ts.dependents.clear()
        ts.waiters.clear()

        for dts in ts.dependencies:
            dts.dependents.remove(ts)
            s = dts.waiters
            s.discard(ts)
            if not dts.dependents and not dts.who_wants:
                # Task not needed anymore
                assert dts is not ts
                recommendations[dts.key] = "forgotten"
        ts.dependencies.clear()
        ts.waiting_on.clear()

        if ts.who_has:
            ts.group.nbytes_in_memory -= ts.get_nbytes()

        for ws in ts.who_has:
            ws.has_what.remove(ts)
            ws.nbytes -= ts.get_nbytes()
            w = ws.address
            if w in self.workers:  # in case worker has died
                self.worker_send(
                    w, {"op": "delete-data", "keys": [key], "report": False}
                )
        ts.who_has.clear()

    def transition_memory_forgotten(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert ts.state == "memory"
                assert not ts.processing_on
                assert not ts.waiting_on
                if not ts.run_spec:
                    # It's ok to forget a pure data task
                    pass
                elif ts.has_lost_dependencies:
                    # It's ok to forget a task with forgotten dependencies
                    pass
                elif not ts.who_wants and not ts.waiters and not ts.dependents:
                    # It's ok to forget a task that nobody needs
                    pass
                else:
                    assert 0, (ts,)

            recommendations = {}

            if ts.actor:
                for ws in ts.who_has:
                    ws.actors.discard(ts)

            self._propagate_forgotten(ts, recommendations)

            self.report_on_key(ts=ts)
            self.remove_key(key)

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_released_forgotten(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert ts.state in ("released", "erred")
                assert not ts.who_has
                assert not ts.processing_on
                assert not ts.waiting_on, (ts, ts.waiting_on)
                if not ts.run_spec:
                    # It's ok to forget a pure data task
                    pass
                elif ts.has_lost_dependencies:
                    # It's ok to forget a task with forgotten dependencies
                    pass
                elif not ts.who_wants and not ts.waiters and not ts.dependents:
                    # It's ok to forget a task that nobody needs
                    pass
                else:
                    assert 0, (ts,)

            recommendations = {}
            self._propagate_forgotten(ts, recommendations)

            self.report_on_key(ts=ts)
            self.remove_key(key)

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition(self, key, finish, *args, **kwargs):
        """ Transition a key from its current state to the finish state

        Examples
        --------
        >>> self.transition('x', 'waiting')
        {'x': 'processing'}

        Returns
        -------
        Dictionary of recommendations for future transitions

        See Also
        --------
        Scheduler.transitions: transitive version of this function
        """
        try:
            try:
                ts = self.tasks[key]
            except KeyError:
                return {}
            start = ts.state
            if start == finish:
                return {}

            if self.plugins:
                dependents = set(ts.dependents)
                dependencies = set(ts.dependencies)

            if (start, finish) in self._transitions:
                func = self._transitions[start, finish]
                recommendations = func(key, *args, **kwargs)
            elif "released" not in (start, finish):
                func = self._transitions["released", finish]
                assert not args and not kwargs
                a = self.transition(key, "released")
                if key in a:
                    func = self._transitions["released", a[key]]
                b = func(key)
                a = a.copy()
                a.update(b)
                recommendations = a
                start = "released"
            else:
                raise RuntimeError(
                    "Impossible transition from %r to %r" % (start, finish)
                )

            finish2 = ts.state
            self.transition_log.append((key, start, finish2, recommendations, time()))
            if self.validate:
                logger.debug(
                    "Transitioned %r %s->%s (actual: %s).  Consequence: %s",
                    key,
                    start,
                    finish2,
                    ts.state,
                    dict(recommendations),
                )
            if self.plugins:
                # Temporarily put back forgotten key for plugin to retrieve it
                if ts.state == "forgotten":
                    try:
                        ts.dependents = dependents
                        ts.dependencies = dependencies
                    except KeyError:
                        pass
                    self.tasks[ts.key] = ts
                for plugin in list(self.plugins):
                    try:
                        plugin.transition(key, start, finish2, *args, **kwargs)
                    except Exception:
                        logger.info("Plugin failed with exception", exc_info=True)
                if ts.state == "forgotten":
                    del self.tasks[ts.key]

            if ts.state == "forgotten" and ts.group.name in self.task_groups:
                # Remove TaskGroup if all tasks are in the forgotten state
                tg = ts.group
                if not any(tg.states.get(s) for s in ALL_TASK_STATES):
                    ts.prefix.groups.remove(tg)
                    del self.task_groups[tg.name]

            return recommendations
        except Exception as e:
            logger.exception("Error transitioning %r from %r to %r", key, start, finish)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    @annotate("scheduler_transitions", domain="distributed")
    def transitions(self, recommendations):
        """ Process transitions until none are left

        This includes feedback from previous transitions and continues until we
        reach a steady state
        """
        keys = set()
        recommendations = recommendations.copy()
        while recommendations:
            key, finish = recommendations.popitem()
            keys.add(key)
            new = self.transition(key, finish)
            recommendations.update(new)

        if self.validate:
            for key in keys:
                self.validate_key(key)

    def story(self, *keys):
        """ Get all transitions that touch one of the input keys """
        keys = set(keys)
        return [
            t for t in self.transition_log if t[0] in keys or keys.intersection(t[3])
        ]

    transition_story = story

    def reschedule(self, key=None, worker=None):
        """ Reschedule a task

        Things may have shifted and this task may now be better suited to run
        elsewhere
        """
        try:
            ts = self.tasks[key]
        except KeyError:
            logger.warning(
                "Attempting to reschedule task {}, which was not "
                "found on the scheduler. Aborting reschedule.".format(key)
            )
            return
        if ts.state != "processing":
            return
        if worker and ts.processing_on.address != worker:
            return
        self.transitions({key: "released"})

    ##############################
    # Assigning Tasks to Workers #
    ##############################

    def check_idle_saturated(self, ws, occ=None):
        """ Update the status of the idle and saturated state

        The scheduler keeps track of workers that are ..

        -  Saturated: have enough work to stay busy
        -  Idle: do not have enough work to stay busy

        They are considered saturated if they both have enough tasks to occupy
        all of their threads, and if the expected runtime of those tasks is
        large enough.

        This is useful for load balancing and adaptivity.
        """
        if self.total_nthreads == 0 or ws.status == "closed":
            return
        if occ is None:
            occ = ws.occupancy
        nc = ws.nthreads
        p = len(ws.processing)

        avg = self.total_occupancy / self.total_nthreads

        if p < nc or occ / nc < avg / 2:
            self.idle.add(ws)
            self.saturated.discard(ws)
        else:
            self.idle.discard(ws)

            pending = occ * (p - nc) / p / nc
            if p > nc and pending > 0.4 and pending > 1.9 * avg:
                self.saturated.add(ws)
            else:
                self.saturated.discard(ws)

    def valid_workers(self, ts):
        """ Return set of currently valid workers for key

        If all workers are valid then this returns ``True``.
        This checks tracks the following state:

        *  worker_restrictions
        *  host_restrictions
        *  resource_restrictions
        """
        s = True

        if ts.worker_restrictions:
            s = {w for w in ts.worker_restrictions if w in self.workers}

        if ts.host_restrictions:
            # Resolve the alias here rather than early, for the worker
            # may not be connected when host_restrictions is populated
            hr = [self.coerce_hostname(h) for h in ts.host_restrictions]
            # XXX need HostState?
            ss = [self.host_info[h]["addresses"] for h in hr if h in self.host_info]
            ss = set.union(*ss) if ss else set()
            if s is True:
                s = ss
            else:
                s |= ss

        if ts.resource_restrictions:
            w = {
                resource: {
                    w
                    for w, supplied in self.resources[resource].items()
                    if supplied >= required
                }
                for resource, required in ts.resource_restrictions.items()
            }

            ww = set.intersection(*w.values())

            if s is True:
                s = ww
            else:
                s &= ww

        if s is True:
            return s
        else:
            return {self.workers[w] for w in s}

    def consume_resources(self, ts, ws):
        if ts.resource_restrictions:
            for r, required in ts.resource_restrictions.items():
                ws.used_resources[r] += required

    def release_resources(self, ts, ws):
        if ts.resource_restrictions:
            for r, required in ts.resource_restrictions.items():
                ws.used_resources[r] -= required

    #####################
    # Utility functions #
    #####################

    def add_resources(self, stream=None, worker=None, resources=None):
        ws = self.workers[worker]
        if resources:
            ws.resources.update(resources)
        ws.used_resources = {}
        for resource, quantity in ws.resources.items():
            ws.used_resources[resource] = 0
            self.resources[resource][worker] = quantity
        return "OK"

    def remove_resources(self, worker):
        ws = self.workers[worker]
        for resource, quantity in ws.resources.items():
            del self.resources[resource][worker]

    def coerce_address(self, addr, resolve=True):
        """
        Coerce possible input addresses to canonical form.
        *resolve* can be disabled for testing with fake hostnames.

        Handles strings, tuples, or aliases.
        """
        # XXX how many address-parsing routines do we have?
        if addr in self.aliases:
            addr = self.aliases[addr]
        if isinstance(addr, tuple):
            addr = unparse_host_port(*addr)
        if not isinstance(addr, str):
            raise TypeError("addresses should be strings or tuples, got %r" % (addr,))

        if resolve:
            addr = resolve_address(addr)
        else:
            addr = normalize_address(addr)

        return addr

    def coerce_hostname(self, host):
        """
        Coerce the hostname of a worker.
        """
        if host in self.aliases:
            return self.workers[self.aliases[host]].host
        else:
            return host

    def workers_list(self, workers):
        """
        List of qualifying workers

        Takes a list of worker addresses or hostnames.
        Returns a list of all worker addresses that match
        """
        if workers is None:
            return list(self.workers)

        out = set()
        for w in workers:
            if ":" in w:
                out.add(w)
            else:
                out.update({ww for ww in self.workers if w in ww})  # TODO: quadratic
        return list(out)

    def start_ipython(self, comm=None):
        """Start an IPython kernel

        Returns Jupyter connection info dictionary.
        """
        from ._ipython_utils import start_ipython

        if self._ipython_kernel is None:
            self._ipython_kernel = start_ipython(
                ip=self.ip, ns={"scheduler": self}, log=logger
            )
        return self._ipython_kernel.get_connection_info()

    def worker_objective(self, ts, ws):
        """
        Objective function to determine which worker should get the task

        Minimize expected start time.  If a tie then break with data storage.
        """
        comm_bytes = sum(
            [dts.get_nbytes() for dts in ts.dependencies if ws not in dts.who_has]
        )
        stack_time = ws.occupancy / ws.nthreads
        start_time = comm_bytes / self.bandwidth + stack_time

        if ts.actor:
            return (len(ws.actors), start_time, ws.nbytes)
        else:
            return (start_time, ws.nbytes)

    async def get_profile(
        self,
        comm=None,
        workers=None,
        scheduler=False,
        server=False,
        merge_workers=True,
        start=None,
        stop=None,
        key=None,
    ):
        if workers is None:
            workers = self.workers
        else:
            workers = set(self.workers) & set(workers)

        if scheduler:
            return profile.get_profile(self.io_loop.profile, start=start, stop=stop)

        results = await asyncio.gather(
            *(
                self.rpc(w).profile(start=start, stop=stop, key=key, server=server)
                for w in workers
            )
        )

        if merge_workers:
            response = profile.merge(*results)
        else:
            response = dict(zip(workers, results))
        return response

    async def get_profile_metadata(
        self,
        comm=None,
        workers=None,
        merge_workers=True,
        start=None,
        stop=None,
        profile_cycle_interval=None,
    ):
        dt = profile_cycle_interval or dask.config.get(
            "distributed.worker.profile.cycle"
        )
        dt = parse_timedelta(dt, default="ms")

        if workers is None:
            workers = self.workers
        else:
            workers = set(self.workers) & set(workers)
        results = await asyncio.gather(
            *(self.rpc(w).profile_metadata(start=start, stop=stop) for w in workers)
        )

        counts = [v["counts"] for v in results]
        counts = itertools.groupby(merge_sorted(*counts), lambda t: t[0] // dt * dt)
        counts = [(time, sum(pluck(1, group))) for time, group in counts]

        keys = set()
        for v in results:
            for t, d in v["keys"]:
                for k in d:
                    keys.add(k)
        keys = {k: [] for k in keys}

        groups1 = [v["keys"] for v in results]
        groups2 = list(merge_sorted(*groups1, key=first))

        last = 0
        for t, d in groups2:
            tt = t // dt * dt
            if tt > last:
                last = tt
                for k, v in keys.items():
                    v.append([tt, 0])
            for k, v in d.items():
                keys[k][-1][1] += v

        return {"counts": counts, "keys": keys}

    async def performance_report(self, comm=None, start=None, code=""):
        stop = time()
        # Profiles
        compute, scheduler, workers = await asyncio.gather(
            *[
                self.get_profile(start=start),
                self.get_profile(scheduler=True, start=start),
                self.get_profile(server=True, start=start),
            ]
        )
        from . import profile

        def profile_to_figure(state):
            data = profile.plot_data(state)
            figure, source = profile.plot_figure(data, sizing_mode="stretch_both")
            return figure

        compute, scheduler, workers = map(
            profile_to_figure, (compute, scheduler, workers)
        )

        # Task stream
        task_stream = self.get_task_stream(start=start)
        from .diagnostics.task_stream import rectangles
        from .dashboard.components.scheduler import task_stream_figure

        rects = rectangles(task_stream)
        source, task_stream = task_stream_figure(sizing_mode="stretch_both")
        source.data.update(rects)

        from distributed.dashboard.components.scheduler import (
            BandwidthWorkers,
            BandwidthTypes,
        )

        bandwidth_workers = BandwidthWorkers(self, sizing_mode="stretch_both")
        bandwidth_workers.update()
        bandwidth_types = BandwidthTypes(self, sizing_mode="stretch_both")
        bandwidth_types.update()

        from bokeh.models import Panel, Tabs, Div

        # HTML
        html = """
        <h1> Dask Performance Report </h1>

        <i> Select different tabs on the top for additional information </i>

        <h2> Duration: {time} </h2>

        <h2> Scheduler Information </h2>
        <ul>
          <li> Address: {address} </li>
          <li> Workers: {nworkers} </li>
          <li> Threads: {threads} </li>
          <li> Memory: {memory} </li>
        </ul>

        <h2> Calling Code </h2>
        <pre>
{code}
        </pre>
        """.format(
            time=format_time(stop - start),
            address=self.address,
            nworkers=len(self.workers),
            threads=sum(w.nthreads for w in self.workers.values()),
            memory=format_bytes(sum(w.memory_limit for w in self.workers.values())),
            code=code,
        )
        html = Div(text=html)

        html = Panel(child=html, title="Summary")
        compute = Panel(child=compute, title="Worker Profile (compute)")
        workers = Panel(child=workers, title="Worker Profile (administrative)")
        scheduler = Panel(child=scheduler, title="Scheduler Profile (administrative)")
        task_stream = Panel(child=task_stream, title="Task Stream")
        bandwidth_workers = Panel(
            child=bandwidth_workers.fig, title="Bandwidth (Workers)"
        )
        bandwidth_types = Panel(child=bandwidth_types.fig, title="Bandwidth (Types)")

        tabs = Tabs(
            tabs=[
                html,
                task_stream,
                compute,
                workers,
                scheduler,
                bandwidth_workers,
                bandwidth_types,
            ]
        )

        from bokeh.plotting import save, output_file

        with tmpfile(extension=".html") as fn:
            output_file(filename=fn, title="Dask Performance Report")
            save(tabs, filename=fn)

            with open(fn) as f:
                data = f.read()

        return data

    async def get_worker_logs(self, comm=None, n=None, workers=None, nanny=False):
        results = await self.broadcast(
            msg={"op": "get_logs", "n": n}, workers=workers, nanny=nanny
        )
        return results

    ###########
    # Cleanup #
    ###########

    def reevaluate_occupancy(self, worker_index=0):
        """ Periodically reassess task duration time

        The expected duration of a task can change over time.  Unfortunately we
        don't have a good constant-time way to propagate the effects of these
        changes out to the summaries that they affect, like the total expected
        runtime of each of the workers, or what tasks are stealable.

        In this coroutine we walk through all of the workers and re-align their
        estimates with the current state of tasks.  We do this periodically
        rather than at every transition, and we only do it if the scheduler
        process isn't under load (using psutil.Process.cpu_percent()).  This
        lets us avoid this fringe optimization when we have better things to
        think about.
        """
        DELAY = 0.1
        try:
            if self.status == "closed":
                return

            last = time()
            next_time = timedelta(seconds=DELAY)

            if self.proc.cpu_percent() < 50:
                workers = list(self.workers.values())
                for i in range(len(workers)):
                    ws = workers[worker_index % len(workers)]
                    worker_index += 1
                    try:
                        if ws is None or not ws.processing:
                            continue
                        self._reevaluate_occupancy_worker(ws)
                    finally:
                        del ws  # lose ref

                    duration = time() - last
                    if duration > 0.005:  # 5ms since last release
                        next_time = timedelta(seconds=duration * 5)  # 25ms gap
                        break

            self.loop.add_timeout(
                next_time, self.reevaluate_occupancy, worker_index=worker_index
            )

        except Exception:
            logger.error("Error in reevaluate occupancy", exc_info=True)
            raise

    def _reevaluate_occupancy_worker(self, ws):
        """ See reevaluate_occupancy """
        old = ws.occupancy

        new = 0
        nbytes = 0
        for ts in ws.processing:
            duration = self.get_task_duration(ts)
            comm = self.get_comm_cost(ts, ws)
            ws.processing[ts] = duration + comm
            new += duration + comm

        ws.occupancy = new
        self.total_occupancy += new - old
        self.check_idle_saturated(ws)

        # significant increase in duration
        if (new > old * 1.3) and ("stealing" in self.extensions):
            steal = self.extensions["stealing"]
            for ts in ws.processing:
                steal.remove_key_from_stealable(ts)
                steal.put_key_in_stealable(ts)

    def check_worker_ttl(self):
        now = time()
        for ws in self.workers.values():
            if ws.last_seen < now - self.worker_ttl:
                logger.warning(
                    "Worker failed to heartbeat within %s seconds. Closing: %s",
                    self.worker_ttl,
                    ws,
                )
                self.remove_worker(address=ws.address)

    def check_idle(self):
        if any(ws.processing for ws in self.workers.values()):
            return
        if self.unrunnable:
            return

        if not self.transition_log:
            close = time() > self.time_started + self.idle_timeout
        else:
            last_task = self.transition_log[-1][-1]
            close = time() > last_task + self.idle_timeout

        if close:
            logger.info(
                "Scheduler closing after being idle for %s",
                format_time(self.idle_timeout),
            )
            self.loop.add_callback(self.close)

    def adaptive_target(self, comm=None, target_duration=None):
        """ Desired number of workers based on the current workload

        This looks at the current running tasks and memory use, and returns a
        number of desired workers.  This is often used by adaptive scheduling.

        Parameters
        ----------
        target_duration: str
            A desired duration of time for computations to take.  This affects
            how rapidly the scheduler will ask to scale.

        See Also
        --------
        distributed.deploy.Adaptive
        """
        if target_duration is None:
            target_duration = dask.config.get("distributed.adaptive.target-duration")
        target_duration = parse_timedelta(target_duration)

        # CPU
        cpu = math.ceil(
            self.total_occupancy / target_duration
        )  # TODO: threads per worker

        # Avoid a few long tasks from asking for many cores
        tasks_processing = 0
        for ws in self.workers.values():
            tasks_processing += len(ws.processing)

            if tasks_processing > cpu:
                break
        else:
            cpu = min(tasks_processing, cpu)

        if self.unrunnable and not self.workers:
            cpu = max(1, cpu)

        # Memory
        limit_bytes = {addr: ws.memory_limit for addr, ws in self.workers.items()}
        worker_bytes = [ws.nbytes for ws in self.workers.values()]
        limit = sum(limit_bytes.values())
        total = sum(worker_bytes)
        if total > 0.6 * limit:
            memory = 2 * len(self.workers)
        else:
            memory = 0

        target = max(memory, cpu)
        if target >= len(self.workers):
            return target
        else:  # Scale down?
            to_close = self.workers_to_close()
            return len(self.workers) - len(to_close)


def decide_worker(ts, all_workers, valid_workers, objective):
    """
    Decide which worker should take task *ts*.

    We choose the worker that has the data on which *ts* depends.

    If several workers have dependencies then we choose the less-busy worker.

    Optionally provide *valid_workers* of where jobs are allowed to occur
    (if all workers are allowed to take the task, pass True instead).

    If the task requires data communication because no eligible worker has
    all the dependencies already, then we choose to minimize the number
    of bytes sent between workers.  This is determined by calling the
    *objective* function.
    """
    deps = ts.dependencies
    assert all(dts.who_has for dts in deps)
    if ts.actor:
        candidates = all_workers
    else:
        candidates = frequencies([ws for dts in deps for ws in dts.who_has])
    if valid_workers is True:
        if not candidates:
            candidates = all_workers
    else:
        candidates = valid_workers & set(candidates)
        if not candidates:
            candidates = valid_workers
            if not candidates:
                if ts.loose_restrictions:
                    return decide_worker(ts, all_workers, True, objective)
                else:
                    return None
    if not candidates:
        return None

    if len(candidates) == 1:
        return first(candidates)

    return min(candidates, key=objective)


def validate_worker_state(ws):
    for ts in ws.has_what:
        assert ws in ts.who_has, (
            "not in has_what' who_has",
            str(ws),
            str(ts),
            str(ts.who_has),
        )

    for ts in ws.actors:
        assert ts.state in ("memory", "processing")


def validate_state(tasks, workers, clients):
    """
    Validate a current runtime state

    This performs a sequence of checks on the entire graph, running in about
    linear time.  This raises assert errors if anything doesn't check out.
    """
    for ts in tasks.values():
        validate_task_state(ts)

    for ws in workers.values():
        validate_worker_state(ws)

    for cs in clients.values():
        for ts in cs.wants_what:
            assert cs in ts.who_wants, (
                "not in wants_what' who_wants",
                str(cs),
                str(ts),
                str(ts.who_wants),
            )


_round_robin = [0]


def heartbeat_interval(n):
    """
    Interval in seconds that we desire heartbeats based on number of workers
    """
    if n <= 10:
        return 0.5
    elif n < 50:
        return 1
    elif n < 200:
        return 2
    else:
        return 5


class KilledWorker(Exception):
    def __init__(self, task, last_worker):
        super(KilledWorker, self).__init__(task, last_worker)
        self.task = task
        self.last_worker = last_worker


class WorkerStatusPlugin(SchedulerPlugin):
    """
    An plugin to share worker status with a remote observer

    This is used in cluster managers to keep updated about the status of the
    scheduler.
    """

    def __init__(self, scheduler, comm):
        self.bcomm = BatchedSend(interval="5ms")
        self.bcomm.start(comm)

        self.scheduler = scheduler
        self.scheduler.add_plugin(self)

    def add_worker(self, worker=None, **kwargs):
        ident = self.scheduler.workers[worker].identity()
        del ident["metrics"]
        del ident["last_seen"]
        try:
            self.bcomm.send(["add", {"workers": {worker: ident}}])
        except CommClosedError:
            self.scheduler.remove_plugin(self)

    def remove_worker(self, worker=None, **kwargs):
        try:
            self.bcomm.send(["remove", worker])
        except CommClosedError:
            self.scheduler.remove_plugin(self)

    def teardown(self):
        self.bcomm.close()
