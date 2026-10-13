# Agent OS Runtime

Independent owner of execution state, run leases, model/tool receipts, DSH exchange retention and Agent notes. Worker is a separate process/repository and consumes the fixed `execution` module. Go 1.27.2; fixed SDK and committed vendor; no sibling source replacement. No new Runtime release is selected on the live Windows installation yet.

## Boundaries

- `runtime`: submission/management API, current ToolHub governance checks, local and external effect receipts, note lifecycle, event delivery, and the sole schema migration.
- `internal/runstate`: execution transactions and archive acknowledgement. It has no HTTP server, model transport, tool implementation or schema migration.
- `execution`: the `runtime-execution-v1` Worker persistence facade. It exposes no database handle, transaction, raw SQL, migration or local-tool invocation. The dependency closure excludes `runtime`.
- Worker owns polling, Node pipe supervision, model HTTP, ToolHub HTTP and historical-loop compatibility. All three existing engine identifiers remain supported. The source-engine bridge described below is an additional candidate; it is not selected by the default Worker facade or live deployment.

`Exchange` retains the existing replay protocol: canonical intent and first event batch commit together, immutable Storage bytes are acknowledged under a valid fence, then model/ToolHub callbacks run outside the transaction. A failed archive barrier stops external work. Completed receipts replay without repeating effects. A response committed before a failed run checkpoint is recovered on replay. `CompleteModel` must finish the model ledger transaction before returning success; it receives the adjusted run/token cap explicitly.

Runtime notes, child creation, tool receipts and events still commit in one transaction. No Registry, Knowledge, Storage or Sandbox database fallback is present. Missing remote configuration fails rather than reading those domains locally. Unknown external writes require reconciliation; a missing reply is not proof of non-execution.

## Build and operate

`GOWORK=off go test -mod=vendor ./...`; `go vet -mod=vendor ./...`; `pwsh ./scripts/Build.ps1`. The build emits separate `runtime`, `migrate`, and `healthcheck` executables. Docker builds from committed vendor and runs non-root. Copy `deploy/service.example.json` to a protected runtime directory and fill only that service's configuration. Never commit real configuration.

Provision distinct existing application roles, then run the migration as an administrator using `MIGRATION_DATABASE_URL` and `migrate --role <runtime-role> --worker-role <worker-role>`. Runtime never runs DDL on startup. Migration retains existing `public` table locations and lock 8843921; schema, note triggers, table grants and sequence grants commit together, with bounded lock/statement deadlines. Existing data and unrelated tables are retained; there is no automatic down migration or legacy-grant revocation.

Fresh Runtime roles receive DML on the original 14 owned tables, SELECT/INSERT only on the new `run_working_states` ledger, and the two event/audit sequences. Fresh Worker roles receive SELECT/UPDATE on runs; SELECT/INSERT/UPDATE on calls, model_attempts, harness_exchanges and harness_archives; SELECT/INSERT on events and sequence usage. Workers do not write notes, submit child runs or reconcile tools directly, and have no access to the working-state ledger.

**Rollback permission condition:** the old monorepo Worker readiness probe reads all Runtime tables, including documents, note lifecycle tables, instances, business_runs and app_runs. Retain those historical SELECT grants while that executable is a rollback target. The fresh minimum Worker account is for the new executable; it is not proof that the old binary can start. The existing Windows account's broader grants are retained, not declared retired.

Local readiness checks owned schema/permissions. It does not prove a downstream/model provider is available. Each process uses a bounded pool (default 3, minimum 2) and HTTP concurrency. This release does not establish final-scale capacity or cross-host HA.

## Structured working state candidate

`runtime/working_state.go` adds a bounded, versioned ledger for derived questions,
observations, plans, third-party forecasts, conflicts and next steps. Original task
and current user constraints remain authoritative in `runs`, read in one SQL
snapshot alongside the ledger; they cannot be supplied by a working-set update.
The ledger is separate from `runs.state`, which historical Worker checkpoints
replace in full. A later constraint revision marks a prior working set stale.

The internal write primitive requires a live run lease, owner/fence and current
rule revision. Replacements use expected-revision CAS and a run-scoped operation
ID. State, stable response and metadata-only audit event commit together. A retry
returns that operation's original receipt even if newer state revisions exist;
a changed payload under the same ID is rejected. Receipt reads remain possible
after cancellation or takeover, which does not grant any further write rights.
The internal primitive rejects unknown fields, duplicate JSON keys, invalid kinds and oversized
working sets (32 KiB including the reason, bounded lists and text). The hash uses
the validated typed value, excluding replaceable owner/fence identity.

Each observation references 1–8 committed tool receipts from the same run. The
ledger retains their canonical response hashes. This proves retrievability, not
that a tool succeeded or a statement is verified; the three observation kinds
are the agent's classifications. No cross-domain SQL or new automatic deletion
is introduced. All historical revisions remain queryable; the latest read is
bounded, but this does not bound total disk history or DSH memory.

Only the authenticated `control` identity can GET `/v1/runs/{id}/working-state`.
Optional `revision` selects an exact historical revision; `operation_id` instead
selects the committed response. Generic run-read privileges for ToolHub and
Knowledge explicitly do not include this route. There is no control mutation
route or historical-engine integration. The Worker-only candidate transport below
does not by itself enable an Agent capability. Activation requires the pinned
source runner, tool registration, versioned context projection and checkpoint acceptance.
Manual edits also require a future plan-invalidation protocol before exposure.

Migration adds one append-only application table, and readiness requires it.
Candidate release metadata uses the new working-state HTTP/data identifiers;
the historical execution facade and engine/tool sets retain their behavior. Run the Owner
migration and coordinate integration release contracts before any activation.
No deployed version, integration source lock or active task has been switched.

Initial disposable Windows PostgreSQL run `p25-working-state-b7fca4e46ce04e9b9fcc943266b360a4`
passed 40 tests, vet and builds; the two opt-in executable handoff/long-history
tests were explicitly skipped. Independent review then found duplicate JSON
keys could differ between struct validation and map hashing; its failing
reproducer is retained in that directory. After the local parser/hash fix,
`p25-working-state-1f39c61b846f454697d7b6b2e98c0ffe` passed 10 focused state/migration
tests with zero skips, vet and builds. Coverage includes actual concurrent CAS,
receipt replay, stale owner/rule rejection, cancellation lookup, cross-run and
uncommitted source refusal, event-failure rollback, restricted-role denial,
unchanged historical Save behavior and authenticated read isolation. PostgreSQL
fixtures were stopped; these runs used no provider or live service deployment.

Final source validation batches receipt lookups in one bounded SQL query instead
of up to 256 round trips. `p25-working-state-2a0e4c65288e4b0897fac9830d551760`
passed the same 10 tests with zero skips, vet and builds, including multiple
source hashes/order and refusal of mixed valid/foreign references. Earlier
reports remain historical evidence; none establishes measured production latency.

## Source engine bridge candidate

`dsh-source-639ed015-state-v1` adds incremental-evidence `checkpoint`,
`working_state_read` and `working_state_update` frames. Historical engines reject
these methods and retain their model/tool response format. `execution.New` only
claims the three historical engines; `NewSourceEnabled` explicitly admits the new
one after its caller verifies a separately pinned runner. No public submission
option, Worker runner selection or deployment activation is implemented here yet.

The Worker transports state invocations through `POST /v1/source-state` using its
service identity. Runtime checks the live lease/fence, source engine, scoped
numeric operation ID, matching staged intent and confirmed immutable archive.
An update intent pins the originating model's rule revision. A caller cannot
reauthorize an old plan by supplying newer rules. Worker still has no access to
`run_working_states`; the new execution facade exposes no state SQL or business
mutation implementation.

Runtime is the sole writer of each source-state Exchange response. A successful
state revision, metadata-only event and response commit in one transaction. A
definite CAS/schema/source rejection instead commits that negative response;
a read commits its exact snapshot. HTTP reply loss cannot refresh the read or
turn a previously rejected write into success when the state later changes.
Database failures roll everything back. The outer Exchange checks the retained
response against transport bytes and then saves its run checkpoint; it never
overwrites Runtime's response. A subsequent valid owner can replay a completed
response without applying the state effect again. Cancelled or expired owners
cannot use this write path; the control receipt API still reads successful
committed state receipts, not every negative/read bridge response.

New-engine ordinary tool results include the platform-derived operation ID and
a `committed` marker. Skipped old plans have no committed source receipt. Large
results retain the existing artifact reference plus the true receipt; retention
does not imply truth, tool success or business verification. A checkpoint only
acknowledges the existing immutable event manifest, not native Session restoration
or a bound on in-memory DSH history.

Local evidence `p25-working-state-f5cb819206ed4b3caf269903b8440401` passed 45 tests,
vet and executable builds, using disposable PostgreSQL, restricted roles and
actual Runtime HTTP. Five new bridge tests cover atomic response rollback,
lost replies for reads/rejections/success, takeover with changed rules,
cancelled/old-owner denial, intent/archive authorization, response corruption,
opt-in claims and large/skipped tool source labels. Two opt-in actual-Worker
tests were skipped; this run contains no Node source runner, paid model call,
live deployment or CI execution. The first attempt's fixture INSERT missed a
required `attempt` column; its failing report is retained as
`p25-working-state-e590ad43f079449c802d4c2be00c4d65`.

After a policy comment and public `SourceEffects` type alias were added, final
evidence `p25-working-state-1375e48e8b0b435db9f8a9c857d4b2b1` passed 15 focused
state/source/migration tests with zero skips, plus vet/build. All 15 recorded
source hashes match this implementation; the fixture was stopped. Independent
static review found no blocking defect in this bridge. Lost HTTP responses are
simulated by discarding the successful response in the test adapter; no actual
network interruption or new-engine process kill occurred in these tests.

## Verification status

Actual disposable PostgreSQL 18 tests cover concurrent claims/fencing, unknown effects, child cancellation, constraint changes, family model budgets, note revision/retirement and audit replay, partial-DDL/GRANT rollback, restricted Runtime/Worker roles and foreign-domain denial. The public facade test injects a lost archive acknowledgement and an exchange-response failure after a note effect committed, then advances the fence and replays without another effect. Storage/effect callbacks in that test are explicitly synthetic; real service/binary handoff is a separate integration gate.

Local full run `p2-runtime-source-f39048c527b64269ba7eddcf057501d0` passed tests/vet/all three builds, with no skipped tests or model calls. Subsequent governance-route tests are tracked in the integration evidence ledger. The first run `9a9dd3b...` failed because old test HTTP configs omitted the newly explicit policy and a concurrency observer waited for a fourth connection in a three-slot fixture pool. Tests now pass policy and observe on the lock-holder connection under a timeout. The second run `3c288cd...` found a test comparing JSONB bytes with unformatted JSON; it now compares JSON values. Original failure reports remain retained.

Independent clone of fixed module `03bee516` passed real PostgreSQL, vet and all three builds with an empty module cache, GOWORK/GOPROXY off and committed vendor (`p2-runtime-source-251793afc8d7478b88b8232c12668eca`). Its [Linux/Windows CI](https://github.com/euler-gaozl/agent-os-runtime/actions/runs/38268513538) passed. Independent static review found no extraction-induced ordering change; it did not execute tests.

The opt-in `TestWorkerBinaryCompatibilityAfterCommittedEffect` takes absolute `TEST_OLD_WORKER_BINARY`, `TEST_NEW_WORKER_BINARY`, `TEST_OLD_DSH_RUNNER`, `TEST_NEW_DSH_RUNNER`, `AGENTOS_NODE` and a disposable `TEST_DATABASE_URL`. It creates its own database and restricted roles, retains the documented legacy readiness reads, verifies actual connection identity, and kills the first executable after Runtime commits a note/receipt but before the proxy returns the reply. Replacement is paused while running at fence 2 to require the exact stale-fence rejection before completion. All three historical engines and both directions passed on Windows with old integration `5bfc84c9`, new Worker `b07b739` and fixed DSH: `p2-worker-binary-handoff-3b30a659311644a9aaa476619ea0f654`. Each case retained one note, one tool HTTP and two model HTTP requests. The temporary cluster stopped afterward; no paid model calls.

Runtime HTTP and PostgreSQL transactions in this test are real; model, ToolHub proxy and immutable Storage HTTP are fixtures. It does not prove complete deployed service integration or Node process-tree cleanup. The earlier `7b43e83...` proof's stale-fence assertion was too weak because it ran after completion; the later proof requires an active replacement lease and exact ErrFence. Live Windows activation remains a separate gate. Passing source or handoff tests does not mean full Agent platform delivery.

Archive retention coverage now lives in `runtime/archive_retention_test.go`: actual SDK HTTP and restricted Runtime/Worker roles cover a lost manifest reply after byte acceptance, original Unicode segments and provenance across takeover, takeover during upload before final acknowledgement, cancellation before acknowledgement, final failed-turn evidence and unsupported-engine exclusion. Five tests passed on disposable PostgreSQL (`p2-runtime-archive-5a53f17d08a24ac48b4f78c874061621`). Storage bytes are an explicit in-memory HTTP fixture; this is not a Storage disk-durability result. Independent review found the initial takeover assertion rejected too early, so the final test changes ownership inside the upload response boundary.

The opt-in `TestIndependentWorkerEvidencePastFourMiBRecallsOmittedSourceAfterProcessInterruption` uses absolute `TEST_NEW_WORKER_BINARY`, `TEST_NEW_DSH_RUNNER`, `AGENTOS_NODE` and disposable `TEST_DATABASE_URL`. It kills the actual independent Worker after Runtime commits a recall receipt but before the HTTP reply, then starts a replacement under the restricted Worker login. Windows proof `p2-worker-evidence-54cef5d09d3f4caabe0b0bd3804351b3` retained 18,874,181 event bytes in 40 batches through 16 DSH compactions, verified the source was omitted from the model context, rediscovered the receipt and reread the original after takeover at fence 2. No committed provider/recall work repeated. The fixture cluster stopped; paid provider calls were zero. This preserves the old scenario under independent processes but does not establish bounded Node memory or complete child-process cleanup. The first attempt failed because its test configuration omitted the Worker's own service identity; the failure record remains retained.

The original proof's cleanup read a mutated configuration map and overwrote the replacement process log. The helper now captures the instance identity before starting the process. Targeted Windows rerun `p2-worker-evidence-ecfcca442b6d46548ac29750099b4e77` passed with no skips, preserved the same recovery assertions, and retained separate nonempty first/replacement logs with hashes. The local runner verifies both logs and records both test-source hashes. This fixes diagnostic provenance; it does not add a new product capability or claim complete Node process-tree cleanup.
