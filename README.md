# Agent OS Runtime

Independent owner of execution state, run leases, model/tool receipts, DSH exchange retention and Agent notes. Worker is a separate process/repository and consumes the fixed `execution` module. Go 1.27.2; fixed SDK and committed vendor; no sibling source replacement. No new Runtime release is selected on the live Windows installation yet.

## Boundaries

- `runtime`: submission/management API, current ToolHub governance checks, local and external effect receipts, note lifecycle, event delivery, and the sole schema migration.
- `internal/runstate`: execution transactions and archive acknowledgement. It has no HTTP server, model transport, tool implementation or schema migration.
- `execution`: the `runtime-execution-v1` Worker persistence facade. It exposes no database handle, transaction, raw SQL, migration or local-tool invocation. The dependency closure excludes `runtime`.
- Worker owns polling, Node pipe supervision, model HTTP, ToolHub HTTP and historical-loop compatibility. All three existing engine identifiers remain supported. DSH source/storage changes are a later separately validated version; this extraction does not implement them.

`Exchange` retains the existing replay protocol: canonical intent and first event batch commit together, immutable Storage bytes are acknowledged under a valid fence, then model/ToolHub callbacks run outside the transaction. A failed archive barrier stops external work. Completed receipts replay without repeating effects. A response committed before a failed run checkpoint is recovered on replay. `CompleteModel` must finish the model ledger transaction before returning success; it receives the adjusted run/token cap explicitly.

Runtime notes, child creation, tool receipts and events still commit in one transaction. No Registry, Knowledge, Storage or Sandbox database fallback is present. Missing remote configuration fails rather than reading those domains locally. Unknown external writes require reconciliation; a missing reply is not proof of non-execution.

## Build and operate

`GOWORK=off go test -mod=vendor ./...`; `go vet -mod=vendor ./...`; `pwsh ./scripts/Build.ps1`. The build emits separate `runtime`, `migrate`, and `healthcheck` executables. Docker builds from committed vendor and runs non-root. Copy `deploy/service.example.json` to a protected runtime directory and fill only that service's configuration. Never commit real configuration.

Provision distinct existing application roles, then run the migration as an administrator using `MIGRATION_DATABASE_URL` and `migrate --role <runtime-role> --worker-role <worker-role>`. Runtime never runs DDL on startup. Migration retains existing `public` table locations and lock 8843921; schema, note triggers, table grants and sequence grants commit together, with bounded lock/statement deadlines. Existing data and unrelated tables are retained; there is no automatic down migration or legacy-grant revocation.

Fresh Runtime roles receive DML only on the 14 owned tables and the two event/audit sequences. Fresh Worker roles receive SELECT/UPDATE on runs; SELECT/INSERT/UPDATE on calls, model_attempts, harness_exchanges and harness_archives; SELECT/INSERT on events and sequence usage. Workers do not write notes, submit child runs or reconcile tools directly.

**Rollback permission condition:** the old monorepo Worker readiness probe reads all Runtime tables, including documents, note lifecycle tables, instances, business_runs and app_runs. Retain those historical SELECT grants while that executable is a rollback target. The fresh minimum Worker account is for the new executable; it is not proof that the old binary can start. The existing Windows account's broader grants are retained, not declared retired.

Local readiness checks owned schema/permissions. It does not prove a downstream/model provider is available. Each process uses a bounded pool (default 3, minimum 2) and HTTP concurrency. This release does not establish final-scale capacity or cross-host HA.

## Verification status

Actual disposable PostgreSQL 18 tests cover concurrent claims/fencing, unknown effects, child cancellation, constraint changes, family model budgets, note revision/retirement and audit replay, partial-DDL/GRANT rollback, restricted Runtime/Worker roles and foreign-domain denial. The public facade test injects a lost archive acknowledgement and an exchange-response failure after a note effect committed, then advances the fence and replays without another effect. Storage/effect callbacks in that test are explicitly synthetic; real service/binary handoff is a separate integration gate.

Local full run `p2-runtime-source-f39048c527b64269ba7eddcf057501d0` passed tests/vet/all three builds, with no skipped tests or model calls. Subsequent governance-route tests are tracked in the integration evidence ledger. The first run `9a9dd3b...` failed because old test HTTP configs omitted the newly explicit policy and a concurrency observer waited for a fourth connection in a three-slot fixture pool. Tests now pass policy and observe on the lock-holder connection under a timeout. The second run `3c288cd...` found a test comparing JSONB bytes with unformatted JSON; it now compares JSON values. Original failure reports remain retained.

Independent clone of fixed module `03bee516` passed real PostgreSQL, vet and all three builds with an empty module cache, GOWORK/GOPROXY off and committed vendor (`p2-runtime-source-251793afc8d7478b88b8232c12668eca`). Its [Linux/Windows CI](https://github.com/euler-gaozl/agent-os-runtime/actions/runs/38268513538) passed. Independent static review found no extraction-induced ordering change; it did not execute tests.

The opt-in `TestWorkerBinaryCompatibilityAfterCommittedEffect` takes absolute `TEST_OLD_WORKER_BINARY`, `TEST_NEW_WORKER_BINARY`, `TEST_OLD_DSH_RUNNER`, `TEST_NEW_DSH_RUNNER`, `AGENTOS_NODE` and a disposable `TEST_DATABASE_URL`. It creates its own database and restricted roles, retains the documented legacy readiness reads, verifies actual connection identity, and kills the first executable after Runtime commits a note/receipt but before the proxy returns the reply. Replacement is paused while running at fence 2 to require the exact stale-fence rejection before completion. All three historical engines and both directions passed on Windows with old integration `5bfc84c9`, new Worker `b07b739` and fixed DSH: `p2-worker-binary-handoff-3b30a659311644a9aaa476619ea0f654`. Each case retained one note, one tool HTTP and two model HTTP requests. The temporary cluster stopped afterward; no paid model calls.

Runtime HTTP and PostgreSQL transactions in this test are real; model, ToolHub proxy and immutable Storage HTTP are fixtures. It does not prove complete deployed service integration or Node process-tree cleanup. The earlier `7b43e83...` proof's stale-fence assertion was too weak because it ran after completion; the later proof requires an active replacement lease and exact ErrFence. Live Windows activation remains a separate gate. Passing source or handoff tests does not mean full Agent platform delivery.
