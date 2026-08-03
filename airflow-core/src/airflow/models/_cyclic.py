# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Loop-body (strongly connected component) detection for true cyclic Dags.

A cyclic Dag has a normal, acyclic edge graph plus a separate lane of declared
loop back-edges (see ``task.loop_to``). The loop body a pass re-runs is a
strongly connected component (SCC) over the *union* of both edge lanes: e.g.
``a >> b >> c`` with ``c.loop_to(a)`` forms the SCC ``{a, b, c}``.

This module is pure graph logic over a (serialized) Dag exposing ``task_dict``,
per-task ``downstream_task_ids`` / ``loop_downstream_task_ids``, and
``loop_edge_info``. It is consumed scheduler-side by ``DagRun.update_state``.
"""

from __future__ import annotations

import heapq
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable


def _all_successors(dag: Any, task_id: str) -> set[str]:
    """Successors over the union of normal and loop edges, restricted to real tasks."""
    task = dag.task_dict[task_id]
    succ = set(task.downstream_task_ids) | set(getattr(task, "loop_downstream_task_ids", ()))
    return {s for s in succ if s in dag.task_dict}


def _strongly_connected_components(dag: Any) -> list[list[str]]:
    """
    Tarjan's SCC over the union of normal and loop edges.

    Iterative (explicit stack) to avoid recursion limits on large Dags, mirroring
    the iterative style of the acyclic ``DAG.check_cycle``.
    """
    index_of: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    on_stack: set[str] = set()
    scc_stack: list[str] = []
    result: list[list[str]] = []
    counter = 0

    for root in dag.task_dict:
        if root in index_of:
            continue
        # work_stack holds (task_id, iterator over its successors)
        work_stack: list[tuple[str, Iterable[str]]] = [(root, iter(sorted(_all_successors(dag, root))))]
        index_of[root] = lowlink[root] = counter
        counter += 1
        on_stack.add(root)
        scc_stack.append(root)

        while work_stack:
            node, succ_iter = work_stack[-1]
            advanced = False
            for succ in succ_iter:
                if succ not in index_of:
                    index_of[succ] = lowlink[succ] = counter
                    counter += 1
                    on_stack.add(succ)
                    scc_stack.append(succ)
                    work_stack.append((succ, iter(sorted(_all_successors(dag, succ)))))
                    advanced = True
                    break
                if succ in on_stack:
                    lowlink[node] = min(lowlink[node], index_of[succ])
            if advanced:
                continue

            # All successors of ``node`` processed; pop it.
            work_stack.pop()
            if work_stack:
                parent = work_stack[-1][0]
                lowlink[parent] = min(lowlink[parent], lowlink[node])
            if lowlink[node] == index_of[node]:
                component: list[str] = []
                while True:
                    member = scc_stack.pop()
                    on_stack.discard(member)
                    component.append(member)
                    if member == node:
                        break
                result.append(component)

    return result


def _has_self_loop(dag: Any, task_id: str) -> bool:
    task = dag.task_dict[task_id]
    return task_id in set(task.downstream_task_ids) | set(getattr(task, "loop_downstream_task_ids", ()))


def find_loop_bodies(dag: Any) -> list[frozenset[str]]:
    """
    Return the loop bodies of ``dag`` as SCCs over normal + loop edges.

    A loop body is any SCC with more than one task, or a single task with a
    self-loop. Tasks not in any loop are omitted, so an acyclic Dag returns ``[]``.
    """
    bodies: list[frozenset[str]] = []
    for component in _strongly_connected_components(dag):
        if len(component) > 1 or (len(component) == 1 and _has_self_loop(dag, component[0])):
            bodies.append(frozenset(component))
    return bodies


def intra_scc_order(dag: Any, scc: Iterable[str]) -> list[str]:
    """
    Return the per-pass execution order within a loop body.

    Removing the loop back-edges from an SCC leaves a DAG (the normal edge graph is
    acyclic), whose topological order is the order tasks run within one pass. Ties
    are broken by task_id for determinism.
    """
    members = set(scc)
    in_degree = {t: 0 for t in members}
    normal_adj: dict[str, list[str]] = {t: [] for t in members}
    for t in members:
        for s in dag.task_dict[t].downstream_task_ids:
            if s in members:
                normal_adj[t].append(s)
                in_degree[s] += 1

    ready = [t for t in members if in_degree[t] == 0]
    heapq.heapify(ready)
    order: list[str] = []
    while ready:
        node = heapq.heappop(ready)
        order.append(node)
        for succ in normal_adj[node]:
            in_degree[succ] -= 1
            if in_degree[succ] == 0:
                heapq.heappush(ready, succ)

    if len(order) != len(members):
        raise ValueError(
            f"Loop body {sorted(members)} is not reducible: its normal edges (excluding "
            f"loop back-edges) still contain a cycle. Overlapping/irreducible loops are "
            f"unsupported."
        )
    return order


def loop_edges_of(dag: Any, scc: Iterable[str]) -> list[tuple[str, str, dict]]:
    """Return ``(tail, head, meta)`` loop back-edges whose both ends lie in ``scc``."""
    members = set(scc)
    edges: list[tuple[str, str, dict]] = []
    for tail, heads in getattr(dag, "loop_edge_info", {}).items():
        if tail not in members:
            continue
        for head, meta in heads.items():
            if head in members:
                edges.append((tail, head, meta))
    return edges
