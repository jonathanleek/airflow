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
"""Scheduler-side re-loop behaviour for true cyclic Dags (DagRun.update_state hook)."""

from __future__ import annotations

import datetime

import pytest

from airflow.models.xcom import XComModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState

pytestmark = pytest.mark.db_test


def _make_cyclic_dag(dag_maker, *, max_iterations=3, until=None):
    with dag_maker(
        dag_id="cyc",
        schedule=datetime.timedelta(days=1),
        start_date=timezone.datetime(2026, 1, 1),
        allow_cycles=True,
        serialized=True,
    ) as dag:
        a = EmptyOperator(task_id="a")
        b = EmptyOperator(task_id="b")
        c = EmptyOperator(task_id="c")
        exit_task = EmptyOperator(task_id="exit")
        a >> b >> c >> exit_task
        c.loop_to(a, max_iterations=max_iterations, until=until)
    return dag


def _complete_a_pass(dr, session, *, try_number):
    """Mark the whole loop body {a, b, c} success with the given try_number."""
    for tid in ("a", "b", "c"):
        ti = dr.get_task_instance(tid, session=session)
        ti.state = TaskInstanceState.SUCCESS
        ti.try_number = try_number
        session.merge(ti)
    session.flush()


def _states(dr, session):
    return {ti.task_id: ti.state for ti in dr.get_task_instances(session=session)}


def test_body_is_cleared_to_reloop_when_under_cap(dag_maker, session):
    _make_cyclic_dag(dag_maker, max_iterations=3)
    dr = dag_maker.create_dagrun()
    _complete_a_pass(dr, session, try_number=1)  # pass 1 of 3

    dr.update_state(session=session)

    states = _states(dr, session)
    # Body was cleared (reset for the next pass); the exit task never ran.
    assert states["a"] is None
    assert states["b"] is None
    assert states["c"] is None
    assert states["exit"] is None


def test_body_not_cleared_at_max_iterations(dag_maker, session):
    _make_cyclic_dag(dag_maker, max_iterations=3)
    dr = dag_maker.create_dagrun()
    _complete_a_pass(dr, session, try_number=3)  # cap reached

    dr.update_state(session=session)

    states = _states(dr, session)
    # Loop exits: the body stays success so the run can proceed to the exit task.
    assert states["a"] == TaskInstanceState.SUCCESS
    assert states["c"] == TaskInstanceState.SUCCESS


def test_guard_satisfied_stops_the_loop(dag_maker, session):
    _make_cyclic_dag(
        dag_maker,
        max_iterations=100,
        until={"task_id": "c", "xcom_key": "solved", "equals": True},
    )
    dr = dag_maker.create_dagrun()
    _complete_a_pass(dr, session, try_number=1)
    XComModel.set(key="solved", value=True, task_id="c", dag_id=dr.dag_id, run_id=dr.run_id, session=session)
    session.flush()

    dr.update_state(session=session)

    # Guard satisfied -> body not cleared even though we're well under max_iterations.
    assert _states(dr, session)["a"] == TaskInstanceState.SUCCESS


def test_guard_not_satisfied_continues_the_loop(dag_maker, session):
    _make_cyclic_dag(
        dag_maker,
        max_iterations=100,
        until={"task_id": "c", "xcom_key": "solved", "equals": True},
    )
    dr = dag_maker.create_dagrun()
    _complete_a_pass(dr, session, try_number=1)
    XComModel.set(key="solved", value=False, task_id="c", dag_id=dr.dag_id, run_id=dr.run_id, session=session)
    session.flush()

    dr.update_state(session=session)

    # Guard not satisfied and under the cap -> body cleared to run again.
    assert _states(dr, session)["a"] is None


def test_acyclic_dag_completes_normally(dag_maker, session):
    with dag_maker(
        dag_id="plain",
        schedule=datetime.timedelta(days=1),
        start_date=timezone.datetime(2026, 1, 1),
        serialized=True,
    ):
        a = EmptyOperator(task_id="a")
        b = EmptyOperator(task_id="b")
        a >> b
    dr = dag_maker.create_dagrun()
    for tid in ("a", "b"):
        ti = dr.get_task_instance(tid, session=session)
        ti.state = TaskInstanceState.SUCCESS
        session.merge(ti)
    session.flush()

    dr.update_state(session=session)

    from airflow.utils.state import DagRunState

    assert dr.state == DagRunState.SUCCESS
