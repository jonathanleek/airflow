<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# True cyclic Dags (Track-1 PoC)

This fork lets a Dag contain a **genuine topological cycle**: a back-edge that the
scheduler natively re-executes, so a group of tasks runs again and again until a
condition is met. It is a real cycle in the graph — not a Dag triggering another Dag,
not a `while` loop hidden inside a single task, and not dynamic task mapping.

## Authoring API

```python
from airflow.sdk import DAG, task

with DAG("my_loop", schedule=None, start_date=..., allow_cycles=True) as dag:
    a = inspect()
    b = decide()
    c = act()
    a >> b >> c                 # normal, acyclic edges (the loop body)
    c.loop_to(                  # the back-edge that closes the cycle
        a,
        max_iterations=100,     # mandatory safety cap on passes
        until={"task_id": "act", "xcom_key": "return_value", "equals": True},
    )
    c >> report                 # optional exit out of the loop
```

- **`DAG(allow_cycles=True)`** — opt in. Without it, declaring a loop edge is an error,
  and stock acyclic Dags behave exactly as before.
- **`task.loop_to(target, *, max_iterations, until=None)`** — declare a back-edge from
  this task to `target`. Works on classic operators and TaskFlow results.
  - `max_iterations` (required): hard cap on passes; the loop always stops here even if
    the guard never fires.
  - `until` (optional): a **declarative** exit guard, evaluated scheduler-side after each
    pass. Use the XCom spec `{"task_id", "xcom_key", "equals"}` — the loop exits when
    that task's XCom equals the expected value. `xcom_key` defaults to `"return_value"`
    (a TaskFlow task's return value). `None` means the loop is bounded solely by
    `max_iterations`. Python callables are **not** supported (they can't be serialized).

The **loop body** is the strongly connected component over the normal + loop edges —
so `a >> b >> c` with `c.loop_to(a)` re-runs all of `a, b, c` each pass, in that order.

## How it works

The back-edge is stored in a separate adjacency lane, invisible to cycle validation,
trigger rules, and topological sort — so nothing about normal scheduling changes. Each
pass runs the body normally; then, at the top of `DagRun.update_state` (before the run
can be judged complete or deadlocked), the scheduler clears the whole body so it runs
again — or, if the guard is satisfied or `max_iterations` is reached, lets the run
proceed to the exit task. Clearing uses Airflow's existing rerun machinery, so each
pass is archived in `task_instance_history`.

## Passing data between passes

Clearing a task resets it, so don't rely on a loop task reading its *own* previous XCom.
Use an Airflow **Variable** to carry state from one pass to the next (see
`rubik_solver.py`, which keeps the cube state in a Variable). The exit guard reads the
XCom of the pass that just finished, which is always available.

## Examples

- **`counter_loop.py`** — the "hello world": a two-task body that loops
  `max_iterations` times, then exits. No guard.
- **`rubik_solver.py`** — a Rubik's cube solved as one cyclic Dag: the body
  `inspect_cube -> compute_move -> apply_move` runs once per physical move and loops
  until the cube is solved (the `until` guard), then reports the solution. Requires
  `pip install pycuber`. Watch the Graph view to see the body cycle once per move.

Run either with:

```bash
airflow dags test counter_loop
airflow dags test rubik_solver
```

## Scope / limitations (PoC)

- Sequential, single-token loops (one pass at a time).
- No branching *inside* a loop body — the body takes the same path each pass.
- Loop-body tasks must have `retries=0` (retries would conflate with pass counting).
- Mapped/deferrable tasks inside a loop, nested-loop condensation, and multi-scheduler
  race-hardening are out of scope.
