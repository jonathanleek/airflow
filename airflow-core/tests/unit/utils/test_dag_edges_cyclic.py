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
"""dag_edges emits loop back-edges so cyclic Dags render their cycle in the graph."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from airflow.sdk.bases.operator import BaseOperator
from airflow.serialization.serialized_objects import DagSerialization
from airflow.utils.dag_edges import dag_edges

START = datetime(2026, 1, 1)


def _serialized_cyclic_dag():
    with DAG("g", schedule=None, start_date=START, allow_cycles=True) as dag:
        a = BaseOperator(task_id="a")
        b = BaseOperator(task_id="b")
        c = BaseOperator(task_id="c")
        d = BaseOperator(task_id="d")
        a >> b >> c >> d
        c.loop_to(a, max_iterations=10)
    return DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))


def test_loop_edge_is_emitted_and_flagged():
    edges = dag_edges(_serialized_cyclic_dag())
    loop_edges = [e for e in edges if e.get("is_loop_edge")]
    assert loop_edges == [{"source_id": "c", "target_id": "a", "is_loop_edge": True}]


def test_normal_edges_still_present_and_unflagged():
    edges = dag_edges(_serialized_cyclic_dag())
    normal = {(e["source_id"], e["target_id"]) for e in edges if not e.get("is_loop_edge")}
    assert normal == {("a", "b"), ("b", "c"), ("c", "d")}


def test_acyclic_dag_has_no_loop_edges():
    with DAG("plain", schedule=None, start_date=START) as dag:
        a = BaseOperator(task_id="a")
        b = BaseOperator(task_id="b")
        a >> b
    sd = DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))
    assert all(not e.get("is_loop_edge") for e in dag_edges(sd))
