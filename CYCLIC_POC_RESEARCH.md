# True Cyclic DAGs in Apache Airflow — Research & Planning Pass

Status: research/planning. File paths in §§2–8 were my first-pass (training-memory)
guesses; **§0 below supersedes them** — it is verified against a real checkout of
`apache/airflow` at commit `b316afb4` (main, 2026-08-03), files read directly.

---

## 0. Verified against source (commit b316afb4) — corrections & better design

I read these files directly (sparse checkout in `/tmp/af-src`):
`task-sdk/src/airflow/sdk/definitions/dag.py`,
`airflow-core/src/airflow/models/{dagrun,taskinstance,taskinstancekey,taskinstancehistory}.py`,
`airflow-core/src/airflow/ti_deps/deps/trigger_rule_dep.py`,
`task-sdk/src/airflow/sdk/definitions/_internal/node.py`,
`airflow/utils/dag_cycle_tester.py`.

### Corrections to my earlier claims
1. **The DAG *definition* moved to the Task SDK.** In Airflow 3 the authoring model
   (`DAG`, operators, `>>`/`<<`, cycle check, topological sort) lives in
   `task-sdk/src/airflow/sdk/definitions/`, **not** `airflow-core/models/dag.py`.
   `airflow-core/models/dag.py` is the ORM/scheduler side.
2. **`check_cycle` is now `DAG.check_cycle()`** (`sdk/definitions/dag.py:1134`,
   DFS with `CYCLE_NEW/IN_PROGRESS/DONE`), called from `DAG.validate()` at
   `dag.py:1177`. The old `airflow/utils/dag_cycle_tester.py` is a **deprecated
   shim** (removed-in 3.1). It walks `current_task.get_direct_relative_ids()`.
3. **TI natural key confirmed** `(dag_id, task_id, run_id, map_index)` —
   `taskinstancekey.py:34` `.primary`; `try_number` is *not* in it. **But** the TI
   row also has a surrogate `id` UUID (`uuid7`) that is **regenerated on every
   attempt** (`taskinstance.py:prepare_db_for_next_try` → `self.id = uuid7()`).
4. **`task_instance_history` archival is REAL and automatic on clear** — confirmed:
   `clear_task_instances` (`taskinstance.py:355`) calls `ti.prepare_db_for_next_try`
   (`:384`) which calls `TaskInstanceHistory.record_ti(self)` (`:1036`). So my
   "free per-iteration history" claim holds, now verified.
5. **Clear does exactly what the loop needs:** sets `ti.state = None` (`:418`),
   bumps `ti.max_tries = ti.try_number + task.retries` (`:411`) so the TI is
   runnable again, and (for finished DRs only) sets DR→QUEUED; a RUNNING DR is left
   RUNNING (docstring `:366`). Our loop DR is RUNNING, so untouched. Good.

### The better design this unlocked: **loop edges as a SEPARATE adjacency set**
Relationships are stored as `upstream_task_ids` / `downstream_task_ids` **sets** per
node (`node.py:134–140`, via `_set_relatives`). Two facts fall out:
- `DAG.check_cycle()` only walks `get_direct_relative_ids()` (those sets).
- `TriggerRuleDep` gates **only** on `task.upstream_task_ids` / `task.upstream_list`
  (`trigger_rule_dep.py:110,283,363`).

**So if a declared back-edge is stored in a NEW parallel set (e.g.
`loop_upstream_task_ids`) instead of the normal ones, then with ZERO changes to
existing code:** cycle check passes (never sees it), trigger rules don't gate on it,
and `topological_sort` still works (normal graph stays a genuine DAG). This is
strictly cleaner than my earlier "bypass `check_cycle`" plan — we don't weaken
validation; undeclared cycles are still rejected. The cyclic semantics then come
*entirely* from new scheduler code that computes **SCCs over (normal ∪ loop) edges**.

### The exact scheduler integration point (and the real hazard)
Verified in `dagrun.py::update_state` (`:1244`) — the terminal decision is:
- **success**: `not unfinished.tis and all(... success ...)` over **leaf** TIs
  (`_tis_for_dagrun_state`, `:1161`) — line **1344**.
- **deadlock→fail**: `unfinished.should_schedule and not are_runnable_tasks` — line **1376**.

**Hazard, now concrete:** when a loop body finishes a pass, all its TIs are
`success`. If nothing else is unfinished, `update_state` marks the whole run
**SUCCESS at line 1344** *before* we can loop — or, if the exit task is gated,
declares **deadlock at 1376**. Therefore the "continue → clear the SCC" hook **must
run at the very top of `update_state`, before `task_instance_scheduling_decisions`
is called (`:1293`)**, so that after the clear the exit task never sees the
controller as `success` and the terminal checks see the SCC as unfinished. That
single insertion point is the crux of the PoC.

---

## 1. Goal & scope

We want **genuine topological cycles** in the workflow graph — a back-edge
`C → A` where the scheduler natively re-executes `A` (and the cycle body) when a
token flows around the loop.

Explicitly **out of scope** (the user rejected these as "not true cyclic"):

- **DAG-of-DAGs** — `TriggerDagRunOperator` / `ExternalTaskSensor` chaining, or
  a DAG that re-triggers itself. (New logical run each time; not one cyclic run.)
- **Loop-until-complete workarounds** — a sensor/branch that clears-and-reruns,
  or a `while` loop *inside* a single `PythonOperator`. (The graph stays acyclic;
  the loop is hidden inside one node or one coordinator.)
- **Dynamic Task Mapping (AIP-42)** — runs N *parallel* copies of a task; it is
  fan-out, not feedback. No back-edge, no sequential dependence between copies.

What we *do* want: the graph itself has a back-edge, and the engine treats a
task as something that can **fire multiple times within a single run**, with data
(XCom) flowing from iteration N to N+1.

---

## 2. Why Airflow forbids this today (the load-bearing assumptions)

The "A" in DAG is not cosmetic. Four core invariants all assume acyclicity:

1. **Explicit cycle rejection.** `check_cycle()` in
   `airflow/utils/dag_cycle_tester.py` runs a DFS at parse/serialize time and
   raises `AirflowDagCycleException("Cycle detected in DAG. Faulty task: X to Y")`.
   Called from `DAG.validate()` (`airflow/models/dag.py`).

2. **One TaskInstance per task per run.** `TaskInstance` PK is
   `(dag_id, run_id, task_id, map_index)` (`airflow/models/taskinstance.py`).
   A task runs **exactly once** per DagRun. A cycle inherently needs a task to
   run many times → the data model has no dimension to hold iteration N vs N+1.
   (`try_number` is for *retries of the same logical attempt*, semantically wrong
   to overload for iterations.)

3. **Topological scheduling + trigger rules.** The scheduler computes the
   schedulable set from upstream **terminal states**. `TriggerRuleDep`
   (`airflow/ti_deps/deps/trigger_rule_dep.py`) says e.g. `all_success` = "all
   direct upstreams succeeded." With a cycle `A→B→C→A`, `A` has `C` as an
   upstream and `C` has `A` as an upstream: neither can ever start →
   **guaranteed deadlock** under normal trigger rules. Topological sort
   (`DAG.topological_sort()`) is *undefined* on a cyclic graph.

4. **DagRun completion / deadlock detector.** `DagRun.update_state` +
   `task_instance_scheduling_decisions` (`airflow/models/dagrun.py`) bucket TIs
   into `schedulable_tis` / `unfinished_tis` / `finished_tis`. Rule: if there are
   unfinished TIs but **none are schedulable**, that's a **deadlock** → run fails.
   A cycle "in flight" (waiting to loop back) looks exactly like a deadlock to
   this code. Completion is decided by **leaf tasks** reaching terminal states —
   but a task on a cycle is never a stable leaf.

**Bottom line:** Airflow is a *topological-order batch executor* with a
one-shot-per-task data model. Cycles break the graph model, the data model, the
trigger-rule engine, the deadlock detector, and the completion rule — plus the
UI (Grid view has one cell per `(task, run)`).

---

## 3. Prior art (what everyone else did instead)

- **No accepted AIP for cycles.** Community stance (see discussion #21726,
  Potiuk's "Magic Loop" article) is explicitly: *the graph stays acyclic; use
  Dynamic Task Mapping or re-triggering.* So this is a genuine fork, not a
  waiting-to-be-merged feature.
- **Temporal / Cadence** — durable execution: the workflow is *imperative code*,
  loops are just `for`/`while`, and the engine persists state via event-sourcing.
  This is the "correct" model for arbitrary cyclic control flow. If the hard
  requirement is *cyclic behavior* and not *specifically Airflow*, Temporal is
  the natural fit.
- **Prefect 2/3** — imperative flows; loops are native Python because there is no
  static graph to keep acyclic.
- **AWS Step Functions** — explicit but **bounded** loop states (`Choice` +
  back-transition, with `MaxAttempts`-style guards).
- **Argo Workflows / Flyte** — recursion via sub-templates / dynamic workflows
  (i.e. the DAG-of-DAGs pattern we're rejecting), not native back-edges.
- **Formal models** — *Petri nets / workflow nets* (token marking, transitions
  fire when input places hold tokens) and *Kahn Process Networks* (cyclic
  dataflow with feedback) are the theory that natively supports cycles. Any
  faithful implementation converges on one of these.

Takeaway: every mature system that supports true cycles either (a) abandoned the
static graph for imperative durable code, or (b) adopted token/dataflow
semantics. Airflow chose neither, so we're porting one of them in.

---

## 4. Semantics we must pin down BEFORE coding

A cycle with no exit is an infinite loop. These decisions shape everything:

1. **Termination construct.** There must be a node that routes either *back*
   (loop edge) or *forward* (exit edge) — a `while`-condition node. Proposal: a
   branch-like **`LoopController`** task; its return value picks continue-vs-exit.
   Without this there is no valid run.
2. **Safety valve.** A mandatory `max_loop_iterations` per cycle (mirrors
   `max_active_runs`) so a bug can't run forever. Hard cap + configurable.
3. **Data feedback.** Does iteration N+1 read iteration N's output? Almost
   certainly yes (that's the point of a feedback loop). XCom must be keyed by
   iteration, with a convention like `xcom_pull(..., iteration=-1)`.
4. **Concurrency of tokens.** Simplest: **one token / one iteration at a time**
   (strictly sequential). Allowing multiple in-flight tokens (pipeline
   parallelism) is far harder and full Petri-net territory. **Recommend
   sequential-only for v1.**
5. **Nested / interlocking cycles.** v1: forbid or require they be properly
   nested (reducible loops only). Irreducible CFGs are a research rabbit hole.
6. **Partial-failure policy.** If iteration N of the body fails, does the whole
   run fail, or can the `LoopController` catch and decide? Define up front.

---

## 5. Architectural options (ranked)

### Option A — Runtime unrolling / generational TaskInstances  ★ recommended
Keep the *stored* graph reducible: annotate the back-edge as a **loop edge** that
does **not** participate in normal trigger-rule gating. Add a **`loop_iteration`**
(a.k.a. `generation`) dimension so each pass around the cycle materializes a fresh
set of TIs. When the `LoopController` says "continue," the scheduler
re-materializes the cycle body at `loop_iteration = N+1` (reusing the existing
clear-and-rerun machinery). Behaviorally a true cycle; structurally dynamic
loop-unrolling.

- **Pros:** Reuses the one-TI-per-*generation* model, keeps full history/lineage,
  trigger rules work unchanged *within* an iteration, incremental & forkable.
- **Cons:** New PK dimension → DB migration; unbounded iterations → unbounded rows
  (mitigated by `max_loop_iterations`); Grid UI needs an iteration axis; deadlock
  detector and completion logic must learn about "loop in flight."
- **Effort:** Large but tractable. This is the realistic path.

### Option B — Token / Petri-net execution engine  (most faithful)
Replace "task runs once, gated by upstream terminal states" with a **marking**:
edges hold tokens, a task **fires** when its input places are marked. Natively
supports cycles, loops, and concurrency.

- **Pros:** Truly cyclic, formally grounded, most general.
- **Cons:** Rewrites the scheduler critical section, trigger rules, TI/DagRun data
  model, backfill, clearing, retries, **and the entire UI** (Grid/Graph assume one
  TI per task). Effectively a **new engine grafted onto Airflow** — a multi-quarter
  research project. Overkill unless concurrency of tokens is a hard requirement.

### Option C — State-machine coordinator overlay  (least core surgery)
A driver task advances an explicit state machine persisted in a side table;
"tasks" are state handlers; cycling is the coordinator re-dispatching. Can live in
a plugin/provider without deep core forking.

- **Cons:** This is essentially the *"loop until complete"* pattern the user
  rejected — the scheduler graph stays acyclic; the coordinator only *simulates*
  cycles. List it for honesty, but it does **not** meet the stated bar.

**Recommendation:** **Option A** delivers genuine cyclic *behavior* with a
topology that carries a real back-edge, at a fraction of B's cost. Treat B as the
"north star" only if multi-token concurrency becomes a firm requirement.

---

## 6. Option A — concrete change surface (Airflow 3.x)

Authoring API (target ergonomics):
```python
a = task_a(); b = task_b(); c = loop_controller()
a >> b >> c
c.loop_to(a, max_iterations=100)   # declares the back-edge as a loop edge
c >> task_exit()                   # forward/exit edge
```

| # | Area | File(s) | Change |
|---|------|---------|--------|
| 1 | Cycle validation | `airflow/utils/dag_cycle_tester.py`, `models/dag.py::validate` | Add `DAG(allow_cycles=True)`. Replace hard reject with: cycles allowed **only** if every back-edge is an explicitly declared loop edge terminating at a `LoopController`; reject undeclared/irreducible cycles. |
| 2 | Edge model / DSL | `models/baseoperator.py`, `models/taskmixin.py` | Add loop-edge type + `.loop_to()`. Loop edges are stored but **excluded** from `upstream_task_ids` used for trigger-rule gating. |
| 3 | Topological sort | `models/dag.py::topological_sort` | Sort the **reduced graph** (DAG minus loop edges) / condense SCCs. Used by scheduler, serialization, UI. |
| 4 | Serialization | `serialization/serialized_objects.py` | Serialize loop-edge metadata + `max_loop_iterations`. Scheduler reads the serialized DAG, so this is mandatory. |
| 5 | New TI dimension | `models/taskinstance.py` + `migrations/` | Add `loop_iteration` (default 0) to the PK. Alembic migration. Touches every TI query/join — the widest blast radius. |
| 6 | Trigger rules | `ti_deps/deps/trigger_rule_dep.py`, `utils/trigger_rule.py` | Loop edges don't gate. Add loop semantics: body of iteration N+1 becomes schedulable when `LoopController` at iteration N emits "continue." |
| 7 | Scheduling decisions | `models/dagrun.py::task_instance_scheduling_decisions`, `update_state` | (a) Don't flag "loop in flight" as deadlock; (b) re-materialize body TIs at `loop_iteration+1` on "continue"; (c) redefine completion: run is done when the loop **exits** (controller takes forward edge) and no cycle tokens remain. |
| 8 | Loop controller | new operator (e.g. `operators/loop.py`) | Branch-like op: return value → continue (loop edge) or exit (forward edge). Enforces `max_loop_iterations`. |
| 9 | Clearing / rerun | `models/dag.py::clear`, `taskinstance.clear_task_instances` | Reuse as the re-materialization primitive; make it iteration-aware. |
| 10 | XCom feedback | `models/xcom.py` | Key XCom by `loop_iteration`; `xcom_pull(iteration=-1)` reads previous pass. |
| 11 | UI | `airflow-core/src/airflow/ui` (React) | Grid view: add iteration axis (one cell per `task × run × iteration`). Graph view (dagre) already renders a back-edge fine; label it as a loop edge. |
| 12 | Executor | — | Likely **unaffected**: executors just run whatever TIs exist. All changes are in *which* TIs are created and *when*. |

**Deadlock detector is the trap.** Item #7's rule ("unfinished but nothing
schedulable ⇒ fail") will kill every cyclic run the instant it waits to loop
back. It must learn a new state: *waiting-to-iterate is not deadlock.*

---

## 7. Recommended phasing

- **Phase 0 — Spike (throwaway):** Hard-code a 3-task cycle. Prove you can
  (a) bypass `check_cycle`, (b) re-materialize a TI at a new `loop_iteration`, and
  (c) stop `update_state` from declaring deadlock. **This is the make-or-break;
  do it before anything else.** No API, no UI.
- **Phase 1 — Data model & serialization:** `loop_iteration` column + migration;
  loop-edge type; serialize it. Cycles still driven by test harness.
- **Phase 2 — Scheduler semantics:** trigger-rule exclusion of loop edges,
  re-materialization on "continue," deadlock-detector fix, completion redefinition.
  `LoopController` operator + `max_loop_iterations` enforcement.
- **Phase 3 — Authoring API & XCom feedback:** `allow_cycles`, `.loop_to()`,
  iteration-keyed XCom.
- **Phase 4 — UI:** Grid iteration axis; graph loop-edge styling.
- **Phase 5 — Hardening:** nested loops, failure policy, backfill interaction,
  performance/row-count limits, docs.

**Fork strategy:** vendor a pinned Airflow (3.x) into this repo, keep changes on a
long-lived branch, and gate *everything* behind `allow_cycles` so stock DAGs are
byte-for-byte unaffected. Expect ongoing rebase pain against upstream `main`.

---

## 8. Risks & open questions

- **Upstream will never merge this** (settled community position) → permanent fork
  maintenance cost. Confirm that's acceptable vs. adopting Temporal/Prefect.
- **`loop_iteration` in the PK** ripples through hundreds of TI queries, joins,
  and the REST/Task SDK API — the single biggest source of bugs.
- **Unbounded state growth**; retention/GC for iteration rows.
- **Backfill, `depends_on_past`, datasets/assets, deferrable operators, mapped
  tasks *inside* a loop** — each is a combinatorial interaction to design.
- **Multi-scheduler HA**: re-materialization must be race-safe under the existing
  row-lock ordering (lock DagRun before TaskInstance) or it'll deadlock the DB.

### Decision gates for the user
1. Must it be **Airflow specifically**, or is the real need cyclic behavior
   (→ Temporal/Prefect may be an order of magnitude less work)?
2. **Sequential single-token** cycles (Option A) enough, or is **multi-token
   concurrency** required (→ forces Option B)?
3. Which Airflow version do we fork — pin now.
4. Confirm the **`LoopController` + `max_loop_iterations`** termination model.

---

## 9a. PoC re-scope (decided: must be Airflow, fork OK, PoC-grade)

Goal narrows to: **demonstrate one task firing multiple times around a real
back-edge, with data flowing across iterations, in a forked Airflow.** We can
drop everything that's about production robustness or history fidelity.

### Fork target: **Airflow 3.x** (decided)
Airflow 3 isolates workers behind the Task Execution API — **workers can't touch
the metadata DB directly**. Implication: the loop-control/re-execution logic
**must live in the scheduler**, not in the operator. That's fine — it's arguably
where it belongs anyway, and it removes any worker-side DB coupling. Repo is a
monorepo; PoC touches almost entirely `airflow-core/src/airflow/...`
(`models/{dag,dagrun,taskinstance}.py`, `ti_deps/deps/trigger_rule_dep.py`,
`utils/dag_cycle_tester.py`, `serialization/`). UI is React under
`airflow-core/src/airflow/ui` (untouched for PoC).

**Free bonus in 3.x:** clearing a TI archives the prior attempt into the
**`task_instance_history`** table. So each loop iteration gets archived
automatically → we get **per-iteration history for free**, which 2.x would not.

### The cheap trick: reuse clear-and-rerun instead of a new PK column
Airflow's **clear** already re-executes a task in place: reset TI → `None`, and
the scheduler re-runs it (archiving the previous attempt to `task_instance_history`).
So:

- **No `loop_iteration` column, no DB migration.** The `task_instance_history`
  rows carry the per-iteration record; `try_number` counts iterations. Fine for a PoC.
- **Re-execution engine = scheduler-side `clear_task_instances()`** on the cycle body.

### The loop body is a STRONGLY CONNECTED COMPONENT, not a task
Requirement: multi-node cycles like `A → B → C → A` (user's `A << B << C << A`),
where **the whole 3-task body re-runs each iteration** — not one task looping. The
right abstraction is the **SCC** (strongly connected component). This is the same
theory as topologically sorting a graph with cycles:

- Run **Tarjan's SCC** over the graph (loop edges included). Any SCC with >1 node
  (or a self-edge) is a **loop body**. Nodes not in a nontrivial SCC behave exactly
  like today.
- **Condense** each SCC to a single super-node → the **condensation is a DAG**, so
  the existing topological machinery still orders the *whole graph* at the SCC level.
- **Within an SCC**, removing the **back-edge(s)** (the edges that close the cycle)
  leaves a DAG → that gives the **intra-iteration execution order** (A then B then
  C). The back-edge is what makes iteration N+1 possible.
- **Iteration unit = the entire SCC.** "Continue" clears **all** TIs in the SCC and
  re-runs the whole body in intra-SCC order; "exit" lets the condensed-DAG
  successors run. So `A << B << C << A` runs A→B→C, A→B→C, … each a full pass.

Multiple/nested cycles fall out naturally: an SCC with several back-edges is still
one loop body; properly-nested loops are nested SCCs. (Irreducible/overlapping
loops remain out of scope for the PoC.)

### Termination — plain tasks, one annotated back-edge
`A << B << C << A` as literally written is an **infinite loop with no exit and no
defined entry point** — the engine can't guess which edge closes the loop or when
to stop. So the *nodes* stay plain normal tasks, but **one edge carries an
annotation**: the declared **back-edge + its guard**. No mandatory special
"controller" operator.

```python
a >> b >> c            # normal intra-body edges
c.loop_to(a, until="{{ ti.xcom_pull(task_ids='c')['done'] }}",  # or a callable
          max_iterations=100)
c >> exit_task         # optional forward/exit edge out of the SCC
```

- The **guard** decides continue-vs-exit each pass; it reads XCom from any body
  task (e.g. `c`'s return). Any plain task can thus drive the decision — no special
  node type required. (A convenience `LoopControllerOperator` can wrap this later,
  but it's sugar, not core.)
- **`max_iterations`** is a hard safety kill, independent of the guard.

### Minimal mechanism (Airflow 3)
1. **Bypass validation** for declared back-edges (`allow_cycles=True`; skip
   `check_cycle` only on annotated loop edges — undeclared cycles still rejected).
2. **Back-edges don't gate** trigger rules, so the SCC's entry task can start on
   iteration 0; non-back edges inside the SCC gate normally → correct intra-pass order.
3. Body tasks run on workers via the Task SDK and push their outputs to **XCom**
   through the execution API. No worker-side DB work.
4. **One focused scheduler hook** in `DagRun.update_state` /
   `task_instance_scheduling_decisions` (scheduler has DB access): when an SCC has
   finished a full pass, evaluate the back-edge guard from XCom; if `continue` **and**
   iteration `< max`, atomically **clear the entire SCC** in the same transaction —
   so the deadlock detector never sees an "unfinished-but-unschedulable" window. On
   `exit`, do nothing → condensed-DAG successors (`exit_task`) run, run completes.
5. **`max_iterations`** enforced scheduler-side from `task_instance_history` count —
   authoritative even if a worker misbehaves.
6. **Data feedback:** a body task writes accumulated state to an Airflow **Variable**
   (or a not-cleared XCom key) that the SCC entry task reads next pass — avoids
   "clear wipes the XCom you wanted to feed forward."

### Explicitly cut for the PoC
- `loop_iteration` PK column + Alembic migration (use `try_number`).
- UI work — Graph view already draws the back-edge; Grid shows latest try. Good enough.
- Nested/irreducible loops, backfill, `depends_on_past`, datasets, deferrable ops,
  multi-scheduler HA race-safety, retention/GC.
- Rich per-iteration XCom lineage.

### Do-or-die spike (do this first, ~the whole risk)
Hard-code a **3-node cycle `A → B → C → A`** (a real multi-task SCC body) and prove
the loop: (a) `check_cycle` bypassed for the back-edge; (b) SCC detected/condensed
and intra-pass order A→B→C respected; (c) scheduler clears the **whole SCC** and the
full body re-runs; (d) `update_state` does **not** declare deadlock at the clear
moment; (e) guard/`max_iterations` exits cleanly to a downstream task. If this spike
runs the 3-task body ~5× (15 task executions, archived in `task_instance_history`)
then exits, the PoC is essentially proven; everything else is ergonomics.

**Spike harness = `DAG.test()`, not `airflow standalone`.** `DAG.test()` runs one
DagRun in-process with DB access and drives the *real* scheduling-decision loop
(`update_state` / `task_instance_scheduling_decisions`) — exactly the code we're
modifying — while executing tasks in-process (so worker isolation and XCom-over-API
don't get in the way during development). It's the fastest iterate loop. Promote to
full `airflow standalone` (scheduler + api-server + dag-processor) only once the
spike passes, to validate the real worker→API→scheduler XCom-signal path.

### Immediate next actions
1. Vendor **Airflow 3.x** into this repo (pin the exact tag). Get `DAG.test()`
   working against a trivial example DAG first (proves the tree builds/runs).
2. Land the do-or-die spike behind an `allow_cycles` flag, iterating with `DAG.test()`.
3. Validate end-to-end under `airflow standalone` (real worker XCom signal).
4. Then layer on the `.loop_to()` DSL, `LoopControllerOperator`, and Variable-based
   feedback.

---

## 9. Sources

- [dag_cycle_tester / AirflowDagCycleException (models.dag)](https://airflow.apache.org/docs/apache-airflow/1.10.12/_modules/airflow/models/dag.html)
- [Discussion #21726 — loop condition in Airflow](https://github.com/apache/airflow/discussions/21726)
- ["Magic Loop in Airflow — reloaded" (Jarek Potiuk)](https://medium.com/apache-airflow/magic-loop-in-airflow-reloaded-3e1bd8fb6671)
- [Airflow Improvement Proposals index](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals)
- [models/dagrun.py (main)](https://github.com/apache/airflow/blob/main/airflow-core/src/airflow/models/dagrun.py)
- [TaskInstance & DagRun Lifecycle (DeepWiki)](https://deepwiki.com/apache/airflow/5.3-taskinstance-and-dagrun-lifecycle)
- [taskinstance API (3.0)](https://airflow.apache.org/docs/apache-airflow/3.0.0/_api/airflow/models/taskinstance/index.html)
