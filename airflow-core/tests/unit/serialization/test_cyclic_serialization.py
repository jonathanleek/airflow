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
"""Round-trip serialization of true-cyclic-Dag loop edges (Track-1 PoC)."""

from __future__ import annotations

from datetime import datetime

import pytest

from airflow.sdk import DAG
from airflow.sdk.bases.operator import BaseOperator
from airflow.serialization.serialized_objects import DagSerialization

START = datetime(2026, 1, 1)


@pytest.fixture
def scheduler_dag():
    """Build a >> b >> c >> d with loop edge c -> a, then serialize+deserialize it."""
    with DAG("loop_dag", schedule=None, start_date=START, allow_cycles=True) as dag:
        a = BaseOperator(task_id="a")
        b = BaseOperator(task_id="b")
        c = BaseOperator(task_id="c")
        d = BaseOperator(task_id="d")
        a >> b >> c >> d
        c.loop_to(a, max_iterations=25, until={"task_id": "c", "xcom_key": "stop", "equals": True})
    return DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))


def test_allow_cycles_round_trips(scheduler_dag):
    assert scheduler_dag.allow_cycles is True


def test_loop_edge_info_round_trips(scheduler_dag):
    assert scheduler_dag.loop_edge_info == {
        "c": {"a": {"until": {"task_id": "c", "xcom_key": "stop", "equals": True}, "max_iterations": 25}}
    }


def test_loop_downstream_survives_as_set(scheduler_dag):
    tc = scheduler_dag.task_dict["c"]
    assert tc.loop_downstream_task_ids == {"a"}
    assert isinstance(tc.loop_downstream_task_ids, set)


def test_loop_upstream_rebuilt_on_deserialize(scheduler_dag):
    # loop_upstream is not serialized; it must be rebuilt from loop_downstream.
    assert scheduler_dag.task_dict["a"].loop_upstream_task_ids == {"c"}


def test_normal_edges_unaffected_and_no_leak(scheduler_dag):
    tc = scheduler_dag.task_dict["c"]
    ta = scheduler_dag.task_dict["a"]
    assert tc.downstream_task_ids == {"d"}
    assert "c" not in ta.upstream_task_ids


def test_acyclic_dag_has_empty_loop_lane():
    with DAG("plain", schedule=None, start_date=START) as dag:
        a = BaseOperator(task_id="a")
        b = BaseOperator(task_id="b")
        a >> b
    sd = DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))
    assert sd.loop_edge_info == {}
    assert sd.task_dict["a"].loop_downstream_task_ids == set()
    assert sd.task_dict["b"].loop_upstream_task_ids == set()
