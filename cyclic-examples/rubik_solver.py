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
Rubik's cube solver as a single **true cyclic Dag** (Track-1 cyclic-Dag PoC).

The loop body ``inspect_cube -> compute_move -> apply_move`` runs once per physical
cube move and loops back on itself until the cube is solved, then exits to
``report``. This is a genuine topological cycle -- the back-edge is declared with
``apply_move.loop_to(inspect_cube, ...)`` -- not a sub-Dag or a while-loop hidden in
one task. Watch the Graph view: the three body tasks cycle green->none->running once
per move.

Cube state (scramble + the solving move sequence + moves applied so far) lives in an
Airflow Variable so it survives the clear-and-rerun between passes. The solver is
deliberately simple -- it unwinds the scramble one move at a time -- because the star
of the demo is the cycle, not the solving algorithm.

Requires ``pycuber`` (``pip install pycuber``).
"""

from __future__ import annotations

import json
import random

import pendulum

from airflow.sdk import DAG, Variable, task

STATE_VAR = "rubik_state"
SCRAMBLE_LEN = 8
# Solving takes exactly SCRAMBLE_LEN moves; the cap is a safety net above that.
MAX_MOVES = SCRAMBLE_LEN + 4
MOVES = ["U", "D", "L", "R", "F", "B", "U'", "D'", "L'", "R'", "F'", "B'"]


def _load_state() -> dict:
    return Variable.get(STATE_VAR, default=None, deserialize_json=True)


def _save_state(state: dict) -> None:
    Variable.set(STATE_VAR, json.dumps(state))


def _new_scramble() -> dict:
    from pycuber import Cube, Formula

    while True:
        scramble = " ".join(random.choice(MOVES) for _ in range(SCRAMBLE_LEN))
        cube = Cube()
        cube(scramble)
        if cube != Cube():  # ensure it isn't accidentally already solved
            solution = str(Formula(scramble).reverse()).split()
            return {"scramble": scramble, "solution": solution, "done": 0}


def _rebuild_cube(state: dict):
    """Solved cube -> apply scramble -> apply the solving moves done so far."""
    from pycuber import Cube

    cube = Cube()
    cube(state["scramble"])
    applied = state["solution"][: state["done"]]
    if applied:
        cube(" ".join(applied))
    return cube


def _solved_facelets(cube) -> int:
    from pycuber import Cube

    ref = Cube()
    return sum(
        cube.get_face(f)[i][j].colour == ref.get_face(f)[i][j].colour
        for f in "UDLRFB"
        for i in range(3)
        for j in range(3)
    )


with DAG(
    dag_id="rubik_solver",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    allow_cycles=True,
    tags=["cyclic", "demo"],
) as dag:

    @task
    def inspect_cube():
        state = _load_state()
        if state is None:  # first pass of a fresh run: scramble a cube
            state = _new_scramble()
            _save_state(state)
            print(f"[rubik] scrambled with: {state['scramble']}")
        cube = _rebuild_cube(state)
        remaining = len(state["solution"]) - state["done"]
        print(f"[rubik] inspect: {_solved_facelets(cube)}/54 facelets solved, {remaining} move(s) to go")

    @task
    def compute_move():
        state = _load_state()
        move = state["solution"][state["done"]]
        print(f"[rubik] compute: next move is {move}")
        return move

    @task
    def apply_move() -> bool:
        from pycuber import Cube

        state = _load_state()
        move = state["solution"][state["done"]]
        state["done"] += 1
        _save_state(state)
        solved = _rebuild_cube(state) == Cube()
        print(f"[rubik] apply: {move}  ->  solved={solved}")
        return solved

    @task
    def report():
        state = _load_state()
        print(f"[rubik] SOLVED in {state['done']} moves: {' '.join(state['solution'])}")
        Variable.delete(STATE_VAR)  # reset so the next run scrambles afresh

    inspected = inspect_cube()
    move = compute_move()
    applied = apply_move()
    inspected >> move >> applied >> report()

    # The back-edge: after each move, loop back to inspect the cube -- unless apply_move
    # reports the cube is solved (the exit guard), or we hit the safety cap.
    applied.loop_to(
        inspected,
        until={"task_id": "apply_move", "xcom_key": "return_value", "equals": True},
        max_iterations=MAX_MOVES,
    )
