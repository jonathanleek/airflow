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
"""Tests for true-cyclic-Dag loop edges (Track-1 PoC): the ``loop_to`` lane."""

from __future__ import annotations

from datetime import datetime

import pytest

from airflow.sdk import DAG, task
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.exceptions import AirflowDagCycleException

START = datetime(2026, 1, 1)


def _cyclic_dag(*, allow_cycles: bool = True, retries: int = 0):
    """Build a >> b >> c >> d with a loop back-edge c -> a."""
    with DAG("loop_dag", schedule=None, start_date=START, allow_cycles=allow_cycles) as dag:
        a = BaseOperator(task_id="a", retries=retries)
        b = BaseOperator(task_id="b", retries=retries)
        c = BaseOperator(task_id="c", retries=retries)
        d = BaseOperator(task_id="d")
        a >> b >> c >> d
        c.loop_to(a, max_iterations=25, until={"task_id": "c", "xcom_key": "stop", "equals": True})
    return dag, {"a": a, "b": b, "c": c, "d": d}


def test_loop_edge_stays_out_of_normal_lane():
    _, t = _cyclic_dag()
    # The back-edge is only in the loop lane, never the normal upstream/downstream sets.
    assert t["c"].downstream_task_ids == {"d"}
    assert t["c"].loop_downstream_task_ids == {"a"}
    assert t["a"].upstream_task_ids == set()
    assert t["a"].loop_upstream_task_ids == {"c"}


def test_check_cycle_ignores_loop_edge():
    dag, _ = _cyclic_dag()
    # Must not raise: the only cycle is via the loop lane, invisible to check_cycle.
    dag.check_cycle()


def test_topological_sort_unaffected_by_loop_edge():
    dag, _ = _cyclic_dag()
    order = [t.task_id for t in dag.topological_sort()]
    assert order.index("a") < order.index("b") < order.index("c") < order.index("d")


def test_loop_edge_info_recorded():
    dag, _ = _cyclic_dag()
    assert dag.loop_edge_info == {
        "c": {"a": {"until": {"task_id": "c", "xcom_key": "stop", "equals": True}, "max_iterations": 25}}
    }


def test_validate_passes_with_allow_cycles():
    dag, _ = _cyclic_dag(allow_cycles=True)
    dag.validate()


def test_validate_requires_allow_cycles():
    dag, _ = _cyclic_dag(allow_cycles=False)
    with pytest.raises(ValueError, match="allow_cycles=True"):
        dag.validate()


def test_validate_rejects_retrying_loop_body_task():
    dag, _ = _cyclic_dag(retries=2)
    with pytest.raises(ValueError, match="retries=0"):
        dag.validate()


def test_undeclared_cycle_still_rejected():
    with DAG("bad", schedule=None, start_date=START, allow_cycles=True) as dag:
        x = BaseOperator(task_id="x")
        y = BaseOperator(task_id="y")
        x >> y >> x
    with pytest.raises(AirflowDagCycleException):
        dag.check_cycle()


def test_loop_to_target_must_be_operator():
    with DAG("d", schedule=None, start_date=START, allow_cycles=True):
        c = BaseOperator(task_id="c")
    with pytest.raises(TypeError, match="must be an Operator"):
        c.loop_to("not_an_operator", max_iterations=5)


def test_loop_to_rejects_non_positive_max_iterations():
    with DAG("d", schedule=None, start_date=START, allow_cycles=True):
        a = BaseOperator(task_id="a")
        c = BaseOperator(task_id="c")
        a >> c
    with pytest.raises(ValueError, match="max_iterations"):
        c.loop_to(a, max_iterations=0)


def test_loop_to_rejects_cross_dag():
    with DAG("d1", schedule=None, start_date=START, allow_cycles=True):
        a = BaseOperator(task_id="a")
    with DAG("d2", schedule=None, start_date=START, allow_cycles=True):
        c = BaseOperator(task_id="c")
    with pytest.raises(RuntimeError, match="different Dags"):
        c.loop_to(a, max_iterations=5)


def test_loop_to_works_on_taskflow_xcomargs():
    with DAG("tf", schedule=None, start_date=START, allow_cycles=True) as dag:

        @task
        def a():
            return 1

        @task
        def b(x):
            return x

        xa = a()
        xb = b(xa)
        xa >> xb
        # loop_to on XComArgs should proxy to the underlying operators.
        xb.loop_to(xa, max_iterations=7, until={"task_id": "b", "xcom_key": "return_value", "equals": 1})

    assert dag.task_dict["b"].loop_downstream_task_ids == {"a"}
    assert dag.task_dict["a"].loop_upstream_task_ids == {"b"}
    assert dag.loop_edge_info["b"]["a"]["max_iterations"] == 7
