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
The "hello world" of true cyclic Dags: a two-task loop bounded by ``max_iterations``.

``step >> checkpoint`` forms the loop body; ``checkpoint.loop_to(step, ...)`` closes
the cycle. With no ``until`` guard the loop simply runs ``max_iterations`` passes and
then exits to ``done``.
"""

from __future__ import annotations

import pendulum

from airflow.sdk import DAG, task

with DAG(
    dag_id="counter_loop",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    allow_cycles=True,
    tags=["cyclic", "demo"],
) as dag:

    @task
    def step():
        print("[counter] step")

    @task
    def checkpoint():
        print("[counter] checkpoint")

    @task
    def done():
        print("[counter] loop finished")

    s = step()
    c = checkpoint()
    s >> c >> done()
    c.loop_to(s, max_iterations=5)
