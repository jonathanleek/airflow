# Build Plan — True Cyclic DAGs in Apache Airflow 3 (PoC)

**Audience:** an engineering agent picking this up cold. Read this top to bottom
before writing code. Companion research/rationale doc:
`.context/cyclic-airflow-research.md` (especially **§0**, which is verified against
source and supersedes the earlier speculative sections). This build plan is the
authoritative, self-contained spec.

Source references **re-verified against the `3.3.0` tag** (HEAD `1438ea3587`) on
2026-08-03 — the §4 table below carries the 3.3.0 line numbers. Every cited symbol
and every *consequence* holds on 3.3.0; only line numbers drifted from the original
`main`@`b316afb4` pass (serialization was nearly identical; models/scheduler drifted
more). No design-level divergence found. `task-sdk` is version `1.3.0` at this tag.

---

## STATUS LOG (append-only; newest first)
- **2026-08-03 — Phase 6 GREEN + PoC COMPLETE (all phases 0–6 done).** Loop back-edges
  emitted by `dag_edges` with an `is_loop_edge` flag + field on graph `EdgeResponse`
  (commit `851dfff`), so the Graph view draws the cycle (reactflow renders every payload
  edge). 3 dag_edges tests pass. Distinct dashed *styling* deferred: needs the UI node
  toolchain (no `node_modules` here) to regenerate OpenAPI TS types + build; exact 3-spot
  change documented in `cyclic-examples/README.md`. **All six phases complete** — the
  engine (lane → serde → SCC → update_state hook), the Rubik demo, and graph rendering
  all work end to end on Airflow 3.3.0.
- **2026-08-03 — Phase 5 GREEN (flagship demo works).** Rubik's cube solved as ONE
  cyclic Dag (commit `6b095b8`): body `inspect_cube -> compute_move -> apply_move`
  loops once per move until solved (`until` XCom guard), ran 8 passes → "SOLVED in 8
  moves" → SUCCESS via `DAG.test()`. Minimal `counter_loop` (max_iterations) also green.
  Added `XComArg.loop_to` so TaskFlow reads `apply.loop_to(inspect, ...)` (validated by
  the Rubik run). Cube state carried in a Variable (feedback convention). `cyclic-examples/`
  has both Dags + a README documenting API/scope. XCom-lifetime question resolved
  empirically: the guard reads the *current* pass's XCom correctly each iteration
  (overwrite semantics), so Variables are the right feedback channel. 12 SDK cyclic
  tests pass; xcom_arg regression = 2 pre-existing env errors only. **Next: Phase 6 (UI).**
- **2026-08-03 — Phase 4 GREEN (THE CRUX — full mechanism works end to end).** Real
  `DagRun.update_state` hook landed (commits `96dfb7a` Phase 3, `0c29081` Phase 4).
  Phase 3: `models/_cyclic.py` SCC detection (9 tests). Phase 4: `_reloop_completed_cycles`
  clears a finished loop body before the terminal checks; declarative XCom guard
  (`_loop_exit_guard_satisfied`) + `max_iterations` (via `try_number`, since body
  retries=0). Verified via `DAG.test()`: exits on max_iterations (4 passes), exits on
  guard (pass 3), body failure FAILS the run (no infinite loop), acyclic fast-path
  untouched. 5 formal `dag_maker` tests (`test_dagrun_cyclic.py`) pass.
  **Regression caught & fixed:** my inserted helper methods stole `update_state`'s
  `@provide_session` decorator → 21 `test_dagrun.py` failures; moved the decorator back →
  **195 passed** (1 pre-existing env error). LESSON for future edits near a `def`: check
  for a decorator on the line above before inserting methods before it. **Next: Phase 5.**
- **2026-08-03 — Phase 2 GREEN (serde gate cleared; Phase 4 unblocked).** Loop edges
  now reach the `SerializedDAG` (commit `7a9d386`). Serialize `loop_downstream_task_ids`
  (in `SerializedBaseOperator.get_serialized_fields`; excluded `loop_upstream` from SDK
  serialize; rebuild loop_upstream on deserialize); emit `loop_edge_info` + `allow_cycles`
  on the Dag; **registered all three in `serialization/schema.json`** (the strict `dag`
  def rejected them until then — caught by the test harness, not the raw round-trip).
  Fixed a real regression: `SerializedMappedOperator` has no loop lane → made the
  upstream rebuild defensive (`getattr(..., ())`); mapped-in-loop is out of scope. 6 new
  round-trip tests pass (`test_cyclic_serialization.py`); operator tests unchanged (same
  6 env errors). Note: existing `test_dag_serialization.py`/`test_serialized_objects.py`
  can't run in the lean venv (need `cncf.kubernetes`) — regression covered instead by the
  autouse fixture round-tripping all example/mapped Dags in the new tests. **Next: Phase 3.**
- **2026-08-03 — Phase 1 GREEN.** Loop-edge lane + API landed (commit `4cb2de7` on
  `cyclic-dags-poc`). Added `loop_upstream_task_ids`/`loop_downstream_task_ids` to
  `GenericDAGNode` (real file `shared/dagnode/.../node.py`, reached via the
  `sdk/_shared` **symlink** — stage the real path), kept out of
  `get_direct_relative_ids`; `DAGNode.loop_to(target, *, max_iterations, until=None)`;
  `DAG(allow_cycles=True)` + `loop_edge_info` (mirrors `edge_info`) + `set_loop_edge`;
  `DAG._validate_loop_edges` (requires allow_cycles, endpoints exist, endpoints
  `retries=0`). 11 new tests pass (`task_sdk/definitions/test_cyclic.py`); `test_dag.py`
  green; the 6 `test_operator.py` errors are pre-existing env issues (structlog caplog
  fixture), identical with changes stashed — no regression. **Next: Phase 2 (serde).**
- **2026-08-03 — Phase 0 GREEN (PoC de-risked).** Env bootstrapped: fork clone at
  tag `3.3.0`, branch `cyclic-dags-poc`, lean editable venv (`airflow/.venv`), SQLite
  metadata DB at `airflow/.airflow_home`. Smoke gate passed. Spike proved the crux:
  a hardcoded `update_state` hook clears the body `{inspect,decide,apply}` after each
  pass; the body ran **5×** (15 executions), archived **4** history rows/task, exited
  to `report` **once**, run ended **SUCCESS** (no premature success, no deadlock).
  Spike committed to throwaway branch `phase0-spike` (`33a8c13`); `cyclic-dags-poc`
  restored clean. `DAG.test()` confirmed to drive `update_state` per tick. §4 line
  numbers re-verified against 3.3.0. **Next: Phase 1.**

---

## 0. EXECUTION PROTOCOL (implementing agent: read this first)

You are executing a **settled plan**. The architecture was researched, verified
against real source, reviewed for defects, and ratified by the user. Your job is
implementation, not redesign. When this doc and your own architectural instincts
disagree, this doc wins unless a §0.3 divergence rule applies.

### 0.1 Settled decisions — do NOT revisit or "improve"
1. Iteration mechanism = **clearing** (`clear_task_instances`). No `loop_iteration`
   PK column. No schema migration, period.
2. Loop edges live in a **separate adjacency lane** (`loop_upstream_task_ids` /
   `loop_downstream_task_ids`), invisible to `check_cycle`, trigger rules, and
   topological sort.
3. The re-run hook goes at the **top of `DagRun.update_state`**, before
   `task_instance_scheduling_decisions` is called. Not in any caller, not in
   `_schedule_dag_run`.
4. The guard (`until`) is **declarative** (XCom-ref spec / Jinja string). Never a
   Python callable.
5. `max_iterations` is mandatory; loop-body tasks must have `retries=0`
   (validate and raise).
6. Pass-to-pass feedback via Airflow **Variables** (unless/until the Phase 5
   XCom-lifetime verification proves XCom survives a clear).
7. Sequential single-token loops only. No branching inside the body. Nested loops
   not required — reject them cleanly if cheap, otherwise just document.
8. Fork base: tag **3.3.0**. Everything gated behind `DAG(allow_cycles=True)`;
   stock DAG behavior must remain byte-for-byte identical.

If you become convinced one of these is wrong, **stop and write up why** —
do not improvise an alternative architecture.

### 0.2 Phase gates (hard ordering)
- **Phase 0 before everything.** If its mechanism can't be made to work after
  honest effort, the plan is invalidated → stop and report findings.
- **Phase 2's round-trip test must be green before any Phase 4 work** (the
  scheduler is blind to loop edges until serialization carries them).
- Each phase's acceptance criteria are gates. Never start phase N+1 on a red gate.
- Commit at each green gate (sequence in §9).

### 0.3 Reality-divergence rules
- All §4 citations were verified on `main` @ `b316afb4`; you build on **3.3.0**.
  Line drift is expected. **First task: re-verify every §4 citation against 3.3.0
  and correct this doc in place.**
- Symbol moved/renamed → grep for it, adapt, update this doc. Signature/param
  differences → adapt silently, note it here.
- A §4 **consequence** no longer holds on 3.3.0 (e.g. trigger rules read edges
  from somewhere new, clear no longer archives history) → that's a design-level
  divergence → stop, write up, ask the user.

### 0.4 Escalation triggers (stop and ask the user)
Phase 0 mechanism failure; any §0.3 design-level divergence; anything that would
force a schema migration or worker-side DB access; a phase blowing past ~2× its
apparent scope. Otherwise **keep moving** — the bar is a working conference demo,
not production polish. Prefer done over perfect.

### 0.5 Repo layout & bootstrap
This workspace repo (branch `jonathanleek/airflow-cyclic-dags`) is nearly empty;
planning docs live in gitignored `.context/`.
1. Clone `apache/airflow` into `airflow/` inside the workspace; add `airflow/` to
   the workspace repo's `.gitignore` (nested git stays independent).
2. In the clone: `git checkout -b cyclic-dags-poc 3.3.0`. **All code commits go to
   that branch in the airflow clone**, not the outer repo.
3. Copy this file into the clone as `CYCLIC_POC_PLAN.md` and keep THAT copy
   updated — the plan should live and travel with the code.
4. Dev install (expect adjustment; consult Airflow's contributing docs if it
   fails): `uv venv`, then editable-install `airflow-core`, `task-sdk`, and the
   **standard provider** (`PythonOperator` lives there, not in core), then
   `airflow db migrate` (SQLite default) before using `DAG.test()`.
5. Smoke gate before touching anything: a trivial *acyclic* DAG runs green via
   `DAG.test()`.

### 0.6 Working style
- Track phases with the task tools; report at each gate: phase, gate result, next.
- Tests are pytest, colocated per Airflow's existing layout (`airflow-core/tests/…`,
  `task-sdk/tests/…`). Run the tests you add **plus** existing tests for every file
  you touch (e.g. the dagrun tests for `dagrun.py`). Full suite not required.
- Keep diffs minimal and localized; match surrounding code style; no drive-by
  refactors — every changed line is future rebase pain.

---

## 1. Goal & non-goals

### 1.0 Context & purpose (from the user — read this to make good judgment calls)
- **Primary deliverable: a conference-demo-quality PoC.** The user is expanding an
  existing **Airflow Summit talk** about a Rubik's-cube-solving Airflow project
  (prior art: https://github.com/jonathanleek/airflow_rubik_solver, which used
  multi-DAG orchestration) — the headline demo is that solver **as a single cyclic
  DAG**. A larger dedicated talk may follow next year.
- **Flagship example workload:** Rubik's cube solver — loop body assesses cube
  state → computes next move → applies it; guard = "cube solved"; terminates.
  Future workloads in mind: **LLM/agentic loops** and **PID-controller-like
  mechanisms**. All terminating.
- **"Watch it loop" is a requirement, not a nice-to-have.** The demo must be
  visually legible in the Airflow UI: the back-edge visible in the Graph view, task
  states visibly cycling as passes execute. This puts *minimal* UI work back in
  scope (see Phase 6) — a correction to earlier "no UI work" scoping.
- **Two-track strategy (user-ratified):** Track 1 = THIS plan (clearing-based,
  demo-focused, ship it for the talk). Track 2 = a future, separately-architected
  fork (token/marking-based, supports branching inside loop bodies, multi-token
  concurrency) — out of scope here; do not compromise Track 1 simplicity for
  Track 2 flexibility. Branching inside a loop body is explicitly deferred to Track 2.
- **Version pin: Airflow 3.3.0** (latest stable on PyPI as of 2026-08-03). Fork
  from the `3.3.0` tag. Source facts in this doc were verified on `main`
  (`b316afb4`, 2026-08-03) which is *ahead of* 3.3.0 — **re-verify every cited
  line number against the 3.3.0 tag as your first task** (structure should match;
  drift is expected in line numbers, occasionally in file layout).


**Goal:** a forked Airflow 3 where a DAG can contain a *genuine topological cycle*
across multiple tasks (e.g. `A >> B >> C` with a back-edge `C ⇒ A`), and the
scheduler natively re-executes the **whole cycle body** each iteration, with data
flowing from one pass to the next, until a guard condition or a hard iteration cap
stops it and control exits to downstream tasks.

**This must be a real cycle**, not any of these (all explicitly rejected):
- DAG-of-DAGs (`TriggerDagRunOperator` / self-retriggering).
- A `while` loop hidden inside one task.
- Dynamic Task Mapping (parallel fan-out, no feedback).

**In scope for the PoC:**
- Multi-task cycles (SCCs of size ≥ 2), e.g. `A << B << C << A`.
- Sequential, single-token iteration (one pass at a time).
- A stop guard + hard `max_iterations` cap.
- Data hand-off between passes.
- Per-iteration history (comes for free — see §4).

**Out of scope for the PoC (document, don't build):**
- Multiple concurrent tokens / pipeline-parallel cycles. (Track 2.)
- Branching/skipping *inside* a loop body — the body takes the same path every
  pass; per-pass variation lives inside task logic. (Track 2.)
- Irreducible or overlapping (non-nested) loops.
- UI work beyond Phase 6's minimal back-edge rendering + demo legibility.
- Backfill, `depends_on_past`, datasets/assets, deferrable operators *inside* a loop.
- Multi-scheduler HA race-hardening.
- A DB schema migration (deliberately avoided — see §4).

---

## 2. Mental model (read this before the code map)

Airflow is a **topological-order batch executor**: each task runs **once per run**,
gated by upstream terminal states, and the run ends when leaf tasks finish. Four
subsystems assume acyclicity: cycle validation, the one-TI-per-task data model,
trigger rules, and the deadlock/completion detector.

**The core trick:** store the back-edge in a **separate adjacency lane** so the four
acyclic subsystems never see it, then drive all cyclic behavior from *new* scheduler
code that (a) computes the loop body as a **strongly connected component (SCC)** over
the *union* of normal + loop edges, and (b) at the top of each scheduling tick,
**clears** (Airflow's existing re-run primitive) the whole SCC when its guard says
"continue." "Clearing" a task resets it to run again and archives the prior attempt
to history — so no new DB column is needed.

Five pillars (each a phase in §6):
1. **Loop-edge lane** — a new `loop_upstream_task_ids` / `loop_downstream_task_ids`
   set + a `.loop_to()` API. Kept out of the normal edge sets.
2. **Serialization** — carry the loop lane through DAG serialization (it will NOT
   ride along automatically).
3. **SCC detection** — compute loop bodies over normal ∪ loop edges, scheduler-side.
4. **The `update_state` hook** — the one real scheduler change; clears the SCC on
   "continue" before the run can be judged complete. *This is the crux.*
5. **Guard + `max_iterations` + data feedback** — termination and pass-to-pass data.

---

## 3. Environment setup

```bash
# 1. Fork target: Airflow 3.3.0 (decided — latest stable as of 2026-08-03).
#    The FORK should be a full clone you commit into this repo.
git clone https://github.com/apache/airflow.git
cd airflow && git checkout 3.3.0   # verify exact tag name via `git tag -l '3.3*'`

# 2. Local dev install (editable). Airflow 3 is a monorepo (airflow-core, task-sdk).
#    Use uv or pip per the repo's contributing docs; install airflow-core + task-sdk
#    editable so edits take effect.

# 3. Fast iteration harness = DAG.test() (see §7). Slow/full = `airflow standalone`.
```

**Key repo geography (Airflow 3 — this surprises people):**
- **Authoring model lives in the Task SDK**, not core:
  `task-sdk/src/airflow/sdk/definitions/` — `dag.py`, operators, `>>`/`<<`
  (`_internal/node.py`), `check_cycle`, `topological_sort`.
- **Scheduler / ORM / execution lives in core:**
  `airflow-core/src/airflow/models/` (`dagrun.py`, `taskinstance.py`, …),
  `airflow-core/src/airflow/jobs/scheduler_job_runner.py`,
  `airflow-core/src/airflow/ti_deps/deps/trigger_rule_dep.py`,
  `airflow-core/src/airflow/serialization/serialized_objects.py`.
- The scheduler operates on a **`SerializedDAG`**, not your Python DAG. Anything the
  scheduler must know (i.e. loop edges) MUST survive serialization (§6.2).

---

## 4. Verified source map (why the approach works)

**Line numbers are for the `3.3.0` tag** (re-verified 2026-08-03). Paths are under
`task-sdk/src/airflow/sdk/` (SDK) and `airflow-core/src/airflow/` (core).

| Fact | Location (tag 3.3.0) | Consequence for us |
|------|----------------------|--------------------|
| `DAG.check_cycle()` walks only `get_direct_relative_ids()` (normal edge sets) | `sdk/definitions/dag.py:1132`, called from `validate()` `:757`→`:1175` | A loop edge kept out of the normal sets is invisible to validation → no bypass needed; undeclared cycles still rejected. |
| Relationships stored as per-node `upstream_task_ids`/`downstream_task_ids` sets | `sdk/definitions/_internal/node.py:87` (`_set_relatives`), adds at `:134–140` | This is where we add the parallel `loop_*` sets. |
| `TriggerRuleDep` gates only on `task.upstream_task_ids` / `upstream_list` | `ti_deps/deps/trigger_rule_dep.py:112,277,357,366` | Loop edges in a separate lane never block scheduling → no deadlock from the back-edge. |
| TI natural key = `(dag_id, task_id, run_id, map_index)`; `try_number` NOT in it | `models/taskinstancekey.py:33–35` (`.primary`) | We do **not** add an iteration column; iterations are re-runs of the same key. |
| Each attempt gets a fresh surrogate `id = uuid7()` | `models/taskinstance.py:1016` (`prepare_db_for_next_try`) | Clearing yields a new row id; prior attempt is archived (below). |
| `clear_task_instances` → `prepare_db_for_next_try` → `TaskInstanceHistory.record_ti` | `models/taskinstance.py:336 → :1010 → :1014` | **Per-iteration history is free.** Clearing archives the pass. |
| Clear sets `state=None` (`:399`), bumps `max_tries = try_number + retries` (`:392`); leaves RUNNING DR alone (docstring) | `models/taskinstance.py:336–410` | Clear is a ready-made "make this task run again" primitive; our RUNNING DagRun is untouched. |
| `update_state` marks **SUCCESS** at `:1249` (all leaf TIs succeeded, none unfinished) and **deadlock→FAIL** at `:1281` (`unfinished.should_schedule and not are_runnable_tasks`) | `models/dagrun.py:1148` (def), decisions call `:1197`, SUCCESS `:1249`, deadlock `:1281` | **The hazard.** A finished loop pass looks like a completed/deadlocked run. Our hook must run **before** the decisions call (before `:1197`). |
| Scheduler path: `_schedule_dag_run` → `update_state` → `schedule_tis` on a `SerializedDAG`; `update_state` ALSO called at `:1658` | `jobs/scheduler_job_runner.py:2789 → :2895 → :2919` (and `:1658`) | Confirms the hook is on the live hot path; hooking inside `update_state` covers both callers. |
| Only `downstream_task_ids` is serialized; upstream is **rebuilt** on deserialize | `serialization/serialized_objects.py:1344,1578,1788` and rebuild at `:1236–1238` | Loop edges need an **explicit** serialized field + a mirror rebuild step. They do NOT ride along for free. |

---

## 5. Target authoring API (what a DAG author writes)

```python
from airflow.sdk import DAG, task

with DAG("cyclic_demo", ...):
    a = task_a()
    b = task_b()
    c = task_c()          # any normal task; its output drives the guard

    a >> b >> c           # normal intra-body edges (a real DAG so far)
    c.loop_to(            # declares the back-edge in the loop lane
        a,
        until="{{ ti.xcom_pull(task_ids='c')['stop'] }}",  # guard: truthy => exit
        max_iterations=25,                                  # hard safety cap
    )
    c >> done             # optional normal exit edge out of the SCC
```

Notes:
- Nodes stay **plain tasks**. Only the back-edge is annotated. No mandatory special
  operator (a `LoopController` convenience wrapper can come later as sugar).
- `until` must be **declarative** (a Jinja string / an XCom reference spec:
  `{task_id, xcom_key, expected}`) because the scheduler only sees the
  **SerializedDAG** — arbitrary Python callables do not survive serialization.
  Do NOT design the guard as a callable.
- `max_iterations` is enforced by the scheduler from history count, independent of
  the guard, so a buggy guard cannot loop forever.

---

## 6. Work breakdown (phased). Do Phase 0 FIRST.

> Gate everything behind `DAG(..., allow_cycles=True)` (or auto-enable when any
> `.loop_to()` exists) so stock DAGs are byte-for-byte unaffected.

### Phase 0 — Do-or-die spike (proves the whole thing; hardcode, no clean API)
**Goal:** a 3-task cycle `A → B → C → A` runs the full body ~5 times, archives each
pass to history, then exits to a downstream task — driven by the real scheduler
decision path (via `DAG.test()`).

Hardcode as much as needed:
- Manually put the back-edge in a temporary side structure (skip the nice API).
- Manually compute the SCC (or hardcode the member task_ids).
- Implement the `update_state` hook (§6.4) and the clear call.
- Fake the guard with a Variable counter; exit when count == 5.

**Acceptance:** `DAG.test()` on the hardcoded DAG shows A, B, C executing 5× each
(15 executions total). **History math:** archival happens at *clear* time, and the
final pass is never cleared — so 5 passes = 4 clears = **4 rows per task in
`task_instance_history` (12 total)**, with the 5th/final attempt living in
`task_instance`. Do not "fix" a 4-row count into 5 — 4 is correct. Run ends
**SUCCESS** (not deadlocked, not prematurely successful), `done` runs exactly once
at the end.

**First step of this phase — RESOLVED (2026-08-03):** `DAG.test()` (3.3.0:
`task-sdk/.../dag.py:1221`) **does** drive `update_state` per tick. Confirmed at
`dag.py:1395–1397`: `while dr.state == DagRunState.RUNNING:` → `schedulable_tis, _ =
dr.update_state(session=session)`, then it marks those TIs SCHEDULED (bumping
`try_number`) and runs them in-process via `_run_task` (when `use_executor=False`).
So the `update_state` hook is exercised directly; the fallback pytest loop is NOT
needed. Guard-via-Variable is the right call for the spike (Variables are DB-backed
and readable by the scheduler-side hook regardless of task-execution mode).

**If this passes, the PoC is de-risked.** Everything after is ergonomics + plumbing.

---

### Phase 1 — Loop-edge lane + `.loop_to()` API
**Files:** `task-sdk/src/airflow/sdk/definitions/_internal/node.py` (edge sets,
`_set_relatives`); `task-sdk/src/airflow/sdk/definitions/baseoperator.py` /
`abstractoperator.py` (expose `loop_to`, `loop_upstream_list` etc.);
`task-sdk/src/airflow/sdk/definitions/dag.py` (`allow_cycles` flag; make
`check_cycle` ignore the loop lane — it already will, since loop edges aren't in the
normal sets, so mainly add the flag + validation that loop edges are declared).

**Do:**
- Add `loop_upstream_task_ids: set[str]` and `loop_downstream_task_ids: set[str]`
  to the node, populated by a new `loop_to(target, *, until, max_iterations)`.
- Store guard (`until`) and `max_iterations` as loop-edge metadata (a small dataclass
  keyed by `(src, dst)`, e.g. on the DAG or the edge).
- Add `DAG.allow_cycles: bool` (default False). If a loop edge exists and
  `allow_cycles` is False, raise a clear error.
- Confirm `check_cycle()` still passes on a DAG whose ONLY cycle is via loop edges
  (it should, untouched). Add a test asserting an *undeclared* cycle still raises.

**Acceptance:** unit tests: declaring a loop edge keeps `upstream_task_ids` clean;
`topological_sort` still works on the normal graph; undeclared cycles still rejected.

**Risk:** operators are frozen/attrs-based in the SDK — adding fields may require
touching the attrs definition and `__init__`. Check how `downstream_task_ids` is
declared and mirror it exactly.

---

### Phase 2 — Serialization round-trip
**Files:** `airflow-core/src/airflow/serialization/serialized_objects.py`.

**Do (mirror how `downstream_task_ids` is handled):**
- Serialize `loop_downstream_task_ids` per node (see the `_downstream_task_ids`
  field handling at `:1344,1578,1788`).
- Serialize loop-edge metadata (guard + `max_iterations`). `edge_info` (`:1724,1805`)
  is a precedent for DAG-level edge metadata; either extend it or add a dedicated key.
- On deserialize, **rebuild `loop_upstream_task_ids`** from the serialized
  downstream lane, mirroring the normal rebuild at `:1236–1238`.

**Acceptance:** round-trip test — serialize a cyclic DAG, deserialize, assert the
loop lane + guard + `max_iterations` are intact on the `SerializedDAG`. THIS is the
gate that the scheduler will actually see the cycle. Do not proceed to Phase 3
scheduler work until this passes.

---

### Phase 3 — SCC detection (scheduler-side helper)
**Files:** new helper module, e.g.
`airflow-core/src/airflow/models/_cyclic.py` (or under `utils/`).

**Do:**
- Implement Tarjan's SCC over the union of normal + loop edges of a `SerializedDAG`.
- Provide: `find_loop_bodies(dag) -> list[frozenset[task_id]]` (SCCs of size ≥ 2, or
  a self-loop), and `intra_scc_order(scc) -> list[task_id]` (topo order of the SCC
  with loop edges removed — the per-pass execution order).
- Cache per serialized-DAG version (recompute only when the DAG version changes).

**Acceptance:** unit tests on hand-built graphs: simple 3-cycle, a nested loop,
a DAG with no loops (returns empty), and that `intra_scc_order` respects normal edges.

---

### Phase 4 — The `update_state` hook (THE CRUX)
**File:** `airflow-core/src/airflow/models/dagrun.py`, method `update_state`
(3.3.0: `:1148`). Also touch `jobs/scheduler_job_runner.py` only if you need the
SerializedDAG handle (it's already available via `self.get_dag()` inside `update_state`).

**Do — insert at the TOP of `update_state`, before `task_instance_scheduling_decisions`
is called (3.3.0: `:1197`):**

```
for scc in find_loop_bodies(dag):
    body_tis = TIs of this run whose task_id in scc
    if all(ti.state in SUCCESS for ti in body_tis):           # a full pass finished
        iteration = history_count(one representative body TI)  # from task_instance_history
        guard_says_continue = evaluate_guard(scc, run, session)  # reads XCom of guard task
        if guard_says_continue and iteration < scc.max_iterations:
            clear_task_instances(body_tis, session=session,
                                 dag_run_state=False)  # do NOT touch the RUNNING DR state
            # exit task needs no handling: it can only have been scheduled by a
            # prior decision, and decisions run AFTER this hook (see §8 exit-task note)
        # else: fall through -> exit edge successors become schedulable normally
```

**Why the position matters (see §4 hazard):** if you clear *after* the decision, the
exit task may already be scheduled, or the run may already be marked SUCCESS at
`:1344` / deadlock at `:1376`. Clearing first means the subsequent
`task_instance_scheduling_decisions` and terminal checks see the SCC as unfinished
and the exit task's upstream (the controller) as reset → correct behavior.

**Guard evaluation:** the scheduler has DB access; read the guard task's XCom
(`airflow-core/src/airflow/models/xcom.py`) or render the Jinja `until`. Keep it a
small, well-isolated function.

**`max_iterations` enforcement:** count archived attempts of a representative body
task in `task_instance_history` for this run. This is authoritative even if a worker
misbehaves. Beware two subtleties:
- **Off-by-one:** at decision time the just-finished pass is NOT yet in history
  (history rows are written by the clear itself). So
  `completed_passes = history_count + 1`, and the continue condition is
  `completed_passes < max_iterations`. Pin this down with a test in Phase 0 —
  an off-by-one here means one extra or one missing pass.
- **Retries conflate with iterations:** retries also archive to history. For the
  PoC, require `retries=0` on loop-body tasks (validate & raise in `.loop_to()`).

**Race check (verified, good news):** Airflow 2's worker "mini-scheduler"
(`schedule_downstream_tasks`, which let a *worker* schedule downstream tasks
immediately on task completion — it would have raced this hook by scheduling the
exit task before `update_state` ran) **does not exist in Airflow 3** — grep for
`schedule_downstream_tasks` across `airflow-core` and `task-sdk` returns nothing.
All scheduling flows through the scheduler's `update_state` path, so clearing at
the top of `update_state` closes the window. If your pinned tag differs from
`b316afb4`, re-run that grep before relying on this.

**Acceptance:** the Phase 0 scenario now runs through the real, clean code path;
add tests for: exits on guard, exits on `max_iterations`, never deadlocks mid-loop,
`done` runs exactly once, failure inside the body still fails the run.

**Risks / must-check:**
- **Idempotency & re-entrancy:** `update_state` runs every tick and can run under
  multiple schedulers. Make the "clear on continue" decision idempotent — only clear
  when the *entire* body is `success` AND not already cleared this pass. Consider a
  marker (e.g. a per-SCC counter in `DagRun.conf`/a note) to avoid double-clearing.
- **Lock ordering:** existing code locks DagRun before TaskInstance. Keep that order
  in the clear path to avoid DB deadlocks under HA.
- **`_are_premature_tis` / `changed_tis`** (`:1305`, `:1732`) interaction — verify
  the recalculation branch doesn't undo the clear.

---

### Phase 5 — Data feedback + polish
**Do:**
- Convention for pass-to-pass data: body task writes accumulated state to an Airflow
  **Variable**. Rationale — XCom lifetime across a clear is **unverified**:
  `prepare_db_for_next_try` does *not* delete XCom rows (verified — it only archives
  to history, deletes `TaskReschedule`, and regenerates the TI uuid), but XCom is
  likely wiped when the *new attempt starts* (execution-API side, not yet read).
  Two consequences: (a) the guard XCom is safely readable in the `update_state` hook
  because the hook reads it *before* clearing; (b) whether a body task can
  `xcom_pull` the *previous pass's* value at run time is unknown — **verify this
  during Phase 5** (read `api_fastapi/execution_api` for XCom clearing on TI start);
  if XCom survives until the new try starts writing, the Variable convention may be
  unnecessary. Until verified, use Variables. Document the pattern; optionally add
  a tiny helper.
- Optional `LoopController` convenience operator (sugar over `.loop_to()`).
- Docs + example DAGs under the fork's examples.

**The flagship example: Rubik's cube solver as ONE cyclic DAG.** This is the talk
demo — treat it as a first-class deliverable, not test scaffolding. Reference the
user's prior multi-DAG version (https://github.com/jonathanleek/airflow_rubik_solver)
for solver logic to port. Suggested shape:

```
inspect_cube  >>  compute_next_move  >>  apply_move          (loop body)
apply_move.loop_to(inspect_cube,
                   until=<cube is solved>, max_iterations=~200)
apply_move >> report_solution                                 (exit)
```

- **One move per pass.** This is what makes the demo watchable: each loop
  iteration = one physical cube move, so the Graph view visibly cycles once per
  move. Don't batch moves per pass.
- Cube state (compact string encoding) lives in an Airflow **Variable** (fits the
  feedback convention); `report_solution` prints the move sequence.
- Solver logic can be dumb (layer-by-layer) — legibility beats optimality; cap
  `max_iterations` generously (~200) above the method's worst case.
- Also include one *minimal* second example (e.g. a counter loop) as the "hello
  world" for docs/tests.

**Acceptance:** the Rubik's DAG solves a scrambled cube end-to-end under
`airflow standalone`, visibly looping in the UI, exiting on the solved guard;
plus the minimal counter example exits on `max_iterations` in a test.

---

### Phase 6 — Minimal UI: make the loop watchable (demo requirement)
**Why this exists (correction to earlier scoping):** the separate-lane design means
loop edges are **not** in `downstream_task_ids` — so the Graph view, which renders
from the serialized normal edges, **will not draw the back-edge at all**. For a
conference demo, the visible back-edge is the money shot. Earlier drafts claimed
"the graph already draws it" — that was true of the abandoned bypass-`check_cycle`
design, and is **wrong** for this one.

**Do (smallest thing that works):**
- Find where the UI's graph-structure payload is built (Airflow 3 React UI is
  served from `airflow-core/src/airflow/ui`; the structure comes from a FastAPI
  endpoint under `airflow-core/src/airflow/api_fastapi/` — locate the endpoint that
  emits nodes/edges for the Graph view). Add loop edges to that payload with a
  distinguishing flag (e.g. `is_loop_edge: true`).
- Render them with distinct styling (dashed/colored). If styling is fiddly, an
  undifferentiated extra edge is acceptable for v1 — presence beats polish.
- **Live cycling comes free:** the Graph/Grid auto-refresh will show body tasks
  flipping success → none → running each pass (a side-benefit of the clearing
  design), and per-pass logs are reachable via each task's try-number selector.
  Verify both behave sanely during a looping run; no code expected.
- Nice-to-have only if cheap: surface a "pass N" indicator (e.g. in the DagRun
  note or task try badge). Do not build custom UI components for it.

**Acceptance:** during a live run of the Rubik's DAG, the Graph view shows the
back-edge and an observer can watch task states cycle around the loop; prior
passes' logs are accessible per try number.

---

## 7. Testing strategy

1. **`DAG.test()` first (fast inner loop).** It runs one DagRun in-process, drives
   the real `update_state` / scheduling-decision code we modify, and executes tasks
   in-process (sidesteps worker isolation and XCom-over-API during development).
   This is the primary harness through Phases 0–4.
2. **Unit tests** for the pure pieces: SCC helper (Phase 3), serialization round-trip
   (Phase 2), `.loop_to()` graph shape (Phase 1).
3. **`airflow standalone` last (full integration).** Validates the real
   worker → execution API → scheduler path: the guard task actually pushes XCom via
   the Task SDK, the scheduler reads it and clears. Do this once Phase 4 passes under
   `DAG.test()`. This is where Airflow-3 worker isolation could bite — the worker
   only emits XCom; all loop logic stays scheduler-side, so it should hold, but
   confirm the guard XCom is visible to the scheduler at decision time.

**Definition of Done (PoC):** the **Rubik's solver DAG** (a 3-task SCC) solves a
scrambled cube under `airflow standalone`: N full passes (one move each), cube state
flowing across passes, clean exit to `report_solution` when the guard fires, with
`max_iterations` respected as a safety net; each pass recorded in
`task_instance_history` and reachable via per-try logs; **the back-edge is visible
in the Graph view and an observer can watch the loop cycle live** (Phase 6); and a
stock (acyclic) DAG in the same instance is completely unaffected.

---

## 8. Known pitfalls & open questions (hand-off checklist)

- **Serialization is the make-or-break plumbing** (Phase 2). If loop edges don't
  reach the `SerializedDAG`, the scheduler is blind to the cycle. Gate Phase 3+ on it.
- **`update_state` timing** (Phase 4) is the single riskiest change. Everything hinges
  on clearing *before* the terminal/deadlock checks.
- **Re-entrancy / double-clear** under frequent ticks and multi-scheduler. Needs an
  idempotency guard.
- **`try_number` vs iteration count** conflation when a body task also has `retries`.
  Simplest PoC rule: loop-body tasks have `retries=0`.
- **Exit task un-skip:** with no mini-scheduler (see Phase 4 race check) and the
  hook running before scheduling decisions, the exit task should never get scheduled
  mid-loop — it only becomes schedulable in a tick where the hook chose NOT to clear.
  So exit-task clearing should be unnecessary; assert this in a Phase 0/4 test
  (`done` has zero attempts until the final pass) rather than defensively clearing it.
- **`update_state` has more than one caller:** besides the main path
  (`scheduler_job_runner.py:3016`), it is also invoked at
  `scheduler_job_runner.py:1747`. Hooking *inside* `update_state` covers both, which
  is another reason to put the logic there and not in `_schedule_dag_run`. Don't
  relocate the hook to a caller.
- **Deferred/mapped tasks inside a loop** — out of scope; assert/raise if present.
- **UI:** Grid shows only the latest pass per task (earlier passes live in history
  and per-try log views). The back-edge is NOT rendered without Phase 6 — the
  separate-lane design hides it from the graph payload. Phase 6 is required for the
  demo; per-iteration Grid axis remains out of scope (Track 2).
- **Fork maintenance:** upstream will never accept this; expect rebase pain. Keep the
  diff small and localized (the separate-lane design helps).

---

## 9. Suggested commit sequence

1. `feat(sdk): loop-edge lane + DAG.allow_cycles + .loop_to()` (Phase 1, tests)
2. `feat(serde): round-trip loop edges + guard metadata` (Phase 2, round-trip test)
3. `feat(scheduler): SCC loop-body detection helper` (Phase 3, unit tests)
4. `feat(scheduler): re-run loop bodies via clear in update_state` (Phase 4 — the crux)
5. `feat: data-feedback convention + counter example + docs` (Phase 5)
6. `feat(examples): rubik's cube solver as a single cyclic DAG` (Phase 5 flagship)
7. `feat(ui): render loop edges in graph view` (Phase 6)

Land Phase 0 as a throwaway branch first; don't ship it — it exists only to prove the
mechanism before you build the clean version.
