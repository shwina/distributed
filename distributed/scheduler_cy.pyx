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
