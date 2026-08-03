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
"""Tests for loop-body (SCC) detection over cyclic Dags."""

from __future__ import annotations

from datetime import datetime

import pytest

from airflow.models._cyclic import find_loop_bodies, intra_scc_order, loop_edges_of
from airflow.sdk import DAG
from airflow.sdk.bases.operator import BaseOperator
from airflow.serialization.serialized_objects import DagSerialization

START = datetime(2026, 1, 1)


def _ops(dag, names):
    return {n: BaseOperator(task_id=n, dag=dag) for n in names}


def test_acyclic_dag_has_no_loop_bodies():
    with DAG("d", schedule=None, start_date=START) as dag:
        t = _ops(dag, "abc")
        t["a"] >> t["b"] >> t["c"]
    assert find_loop_bodies(dag) == []


def test_simple_three_task_cycle():
    with DAG("d", schedule=None, start_date=START, allow_cycles=True) as dag:
        t = _ops(dag, "abc")
        t["a"] >> t["b"] >> t["c"]
        t["c"].loop_to(t["a"], max_iterations=10)
    assert find_loop_bodies(dag) == [frozenset({"a", "b", "c"})]


def test_intra_scc_order_follows_normal_edges():
    with DAG("d", schedule=None, start_date=START, allow_cycles=True) as dag:
        t = _ops(dag, "abc")
        t["a"] >> t["b"] >> t["c"]
        t["c"].loop_to(t["a"], max_iterations=10)
    assert intra_scc_order(dag, {"a", "b", "c"}) == ["a", "b", "c"]


def test_tasks_outside_the_loop_are_excluded():
    # entry has an upstream, exit has a downstream; only the SCC is a loop body.
    with DAG("d", schedule=None, start_date=START, allow_cycles=True) as dag:
        t = _ops(dag, "abcde")  # pre -> a -> b -> c -> post ; c loops to a
        pre, a, b, c, post = t["a"], t["b"], t["c"], t["d"], t["e"]
        pre >> a >> b >> c >> post
        c.loop_to(a, max_iterations=10)
    assert find_loop_bodies(dag) == [frozenset({"b", "c", "d"})]  # a=pre,b=a,c=b,d=c,e=post


def test_self_loop_single_task():
    with DAG("d", schedule=None, start_date=START, allow_cycles=True) as dag:
        t = _ops(dag, "ab")
        t["a"] >> t["b"]
        t["b"].loop_to(t["b"], max_iterations=10)
    assert find_loop_bodies(dag) == [frozenset({"b"})]
    assert intra_scc_order(dag, {"b"}) == ["b"]


def test_two_disjoint_loops():
    with DAG("d", schedule=None, start_date=START, allow_cycles=True) as dag:
        t = _ops(dag, "abcd")
        t["a"] >> t["b"]
        t["b"].loop_to(t["a"], max_iterations=5)
        t["c"] >> t["d"]
        t["d"].loop_to(t["c"], max_iterations=5)
    assert set(find_loop_bodies(dag)) == {frozenset({"a", "b"}), frozenset({"c", "d"})}


def test_nested_back_edges_collapse_to_one_body():
    # a->b->c->d with an inner back-edge c->b and an outer back-edge d->a:
    # over the edge union all four are one SCC (the PoC treats it as one loop body).
    with DAG("d", schedule=None, start_date=START, allow_cycles=True) as dag:
        t = _ops(dag, "abcd")
        t["a"] >> t["b"] >> t["c"] >> t["d"]
        t["c"].loop_to(t["b"], max_iterations=5)
        t["d"].loop_to(t["a"], max_iterations=5)
    assert find_loop_bodies(dag) == [frozenset({"a", "b", "c", "d"})]
    assert intra_scc_order(dag, {"a", "b", "c", "d"}) == ["a", "b", "c", "d"]


def test_loop_edges_of_returns_metadata():
    with DAG("d", schedule=None, start_date=START, allow_cycles=True) as dag:
        t = _ops(dag, "abc")
        t["a"] >> t["b"] >> t["c"]
        t["c"].loop_to(t["a"], max_iterations=42, until="{{ done }}")
    edges = loop_edges_of(dag, {"a", "b", "c"})
    assert edges == [("c", "a", {"until": "{{ done }}", "max_iterations": 42})]


def test_works_on_serialized_dag():
    # The scheduler runs against a SerializedDAG; detection must work there too.
    with DAG("d", schedule=None, start_date=START, allow_cycles=True) as dag:
        t = _ops(dag, "abc")
        t["a"] >> t["b"] >> t["c"]
        t["c"].loop_to(t["a"], max_iterations=10)
    sd = DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))
    assert find_loop_bodies(sd) == [frozenset({"a", "b", "c"})]
    assert intra_scc_order(sd, {"a", "b", "c"}) == ["a", "b", "c"]
    assert loop_edges_of(sd, {"a", "b", "c"})[0][:2] == ("c", "a")
