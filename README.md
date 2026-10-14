# Agent OS Runtime

Independent owner of execution state, run leases, model/tool receipts, DSH exchange retention and Agent notes. Worker is a separate process/repository and consumes the fixed `execution` module. Go 1.27.2; fixed SDK and committed vendor; no sibling source replacement. Actual Windows activation is recorded by the integration repository; building a later candidate here does not change the deployed release.

## App-scoped retained document reads

The `run_recall` tool accepts its published `limit` parameter for both listing
and operation reads. Listing honors smaller requests and caps pages at the
existing ten receipts; an explicit limit returns `page_size` so callers can
follow `next_cursor` rather than assume their requested count was returned.
Operation reads keep the 1200-character bound, and nonzero offset still requires
an operation ID. Historical successful receipts replay unchanged. This fixes a
real source-harness run that failed on schema-valid `{kind:"tools",limit:50}`;
the replay/scope behavior and data schema are unchanged.

`GET /v1/apps/{app}/runs/{id}/evidence` lists successful `document_read`
receipts from that app task and its descendants (at most 128 visible tasks).
It returns bounded previews, document identifiers and original read ranges;
default page size is 20, maximum 30. `document_id` filters an individual source,
and the seek cursor is bound to the app, root task and document filter.
`.../evidence/record?operation_id=...` reads only retained document text, with
Unicode pagination (maximum 8192 characters) and a pinned response version.
No model request, arbitrary tool payload or document metadata is exposed.
Other tasks/apps and unresolved calls are excluded. The existing futures peer
can use only its app path; it still cannot use management receipt APIs.

These are the pieces the task actually read, not a fresh Knowledge lookup or
a claim that the complete source was read or independently verified. External
capture tools do not yet appear here. The API is additive and needs no schema
migration; older consumers continue unchanged. Readiness does not declare
research quality, and future consumers must show unavailable rather than an
empty evidence list when the API has not been installed.

## Bounded management receipt candidate

The control-only `GET /v1/runs/{id}/calls/page` returns newest-first summaries
(default 25, maximum 50), with a `(created_at,id)` seek cursor bound to the run
and `all`/`unresolved` filter. Summaries exclude request/response bodies; unresolved
calls are queried independently of the history page. This is a live listing,
not a database snapshot: refresh to observe new or late-committing receipts.

`GET /v1/runs/{id}/calls/record` takes `operation_id`, `field` (request, response,
error), `offset`, `limit` and `version`. It returns at most 16,384 Unicode code
points (default 8,192). Subsequent segments require the first segment's content
version; changed text returns 409. SQL NULL differs from saved JSON null. The
version hashes PostgreSQL's persisted text representation, not provider wire
bytes or the working-state canonical JSON hash. Database conversion/hash cost
is proportional to one field; this does not establish a DB memory hard bound.
The historical complete `Calls` API and Worker execution module are unchanged.

The owner migration adds normal and unresolved seek indexes transactionally.
Back up and apply the owner migration before activation; install this Runtime
before a Console requiring `call-receipts-v1`, and roll that Console back first.
Windows release metadata guards this capability and the bound actual local
target. Formal activation and Linux deployment remain separate acceptance steps.

Local evidence `call-reads-3f565d3b33f045b38d3881b137d5452c` records 22 passing
tests/subtests, no skips, vet and independent builds against restricted disposable
PostgreSQL: timestamp ties, foreign cursors, off-page unresolved calls, Unicode
reassembly, version conflicts, null semantics, HTTP roles and related migration,
note and working-state regression. Initial substring parameter typing failure
was retained and fixed. No paid provider or Actions ran.

## Resident-v2 version selection candidate

The additive `execution.NewSourceVersions` constructor accepts only explicitly
configured state-v1/resident-v2 source engines. `New` remains historical-only;
`NewSourceEnabled` remains state-v1-only. Callers must verify each exact pinned
artifact before enabling its claims. No slice supplied by the caller is retained.
Unknown and duplicate versions fail. The source protocol, archive requirements,
state responses, receipt metadata, cancellation and operator recovery support
both explicit source versions. Exchange refuses a caller/DB engine mismatch
before any intent or effect, and checks again under the run row lock.

Deployment configuration may select resident-v2 for new roots, but this is not
permission to activate it: matching Worker pins, package compatibility and
rollback checks are required. Idempotent retries and children preserve the
original accepted engine. No migration or production-default change is included.

Local evidence `p25-resident-runtime-75cd1e2ff8a64746b2cb17ba67b59999` passed
79 records (including subtests), zero test skips, vet and builds using restricted
disposable PostgreSQL and Runtime HTTP. Tests cover both source versions' state,
archive/frozen-reply/recovery/cancellation boundaries, explicit claim combinations,
engine mismatch and public root/child version retention. The archive/effects are
fixtures. Independent read-only review matched all 11 source hashes and found
no blocker; it did not run tests. Worker/Node process acceptance and production
release remain separate. No paid model calls or GitHub Actions ran.

Subsequent Worker acceptance exercised both source engines with actual Node and
Worker processes: three lost-reply/interruption/recovery modes per version, plus
old-Worker to new-Worker takeover of an unchanged v1 task. All seven passed and
were repeated successfully from a relocated complete Worker package (`346c003`),
with the original package directory absent. Evidence:
`p25-worker-resident-9646a337349a4343b6568d2a7c5fa408`. Runtime HTTP and PostgreSQL
are real; model, ToolHub routing and immutable Storage are fixtures. This does
not prove cross-engine migration, production activation or fast native resume.

## Boundaries

- `runtime`: submission/management API, current ToolHub governance checks, local and external effect receipts, note lifecycle, event delivery, and the sole schema migration.
- `internal/runstate`: execution transactions and archive acknowledgement. It has no HTTP server, model transport, tool implementation or schema migration.
- `execution`: the `runtime-execution-v1` Worker persistence facade. It exposes no database handle, transaction, raw SQL, migration or local-tool invocation. The dependency closure excludes `runtime`.
- Worker owns polling, Node pipe supervision, model HTTP, ToolHub HTTP and historical-loop compatibility. All three existing engine identifiers remain supported. The source-engine bridge described below is an additional candidate; it is not selected by the default Worker facade or live deployment.

`Exchange` retains the existing replay protocol: canonical intent and first event batch commit together, immutable Storage bytes are acknowledged under a valid fence, then model/ToolHub callbacks run outside the transaction. A failed archive barrier stops external work. Completed receipts replay without repeating effects. A response committed before a failed run checkpoint is recovered on replay. `CompleteModel` must finish the model ledger transaction before returning success; it receives the adjusted run/token cap explicitly.

Runtime notes, child creation, tool receipts and events still commit in one transaction. No Registry, Knowledge, Storage or Sandbox database fallback is present. Missing remote configuration fails rather than reading those domains locally. Unknown external writes require reconciliation; a missing reply is not proof of non-execution.

## Build and operate

`GOWORK=off go test -mod=vendor ./...`; `go vet -mod=vendor ./...`; `pwsh ./scripts/Build.ps1`. The build emits separate `runtime`, `migrate`, `retire-access`, and `healthcheck` executables. Docker builds from committed vendor and runs non-root. Copy `deploy/service.example.json` to a protected runtime directory and fill only that service's configuration. Never commit real configuration.

Provision distinct existing application roles, then run the migration as an administrator using `MIGRATION_DATABASE_URL` and `migrate --role <runtime-role> --worker-role <worker-role>`. Runtime never runs DDL on startup. Migration retains existing `public` table locations and lock 8843921; schema, note triggers, table grants and sequence grants commit together, with bounded lock/statement deadlines. Existing data and unrelated tables are retained; there is no automatic down migration or legacy-grant revocation.

Fresh Runtime roles receive DML on the original 14 owned tables, SELECT/INSERT only on the new `run_working_states` ledger, and the two event/audit sequences. Fresh Worker roles receive SELECT/UPDATE on runs; SELECT/INSERT/UPDATE on calls, model_attempts, harness_exchanges and harness_archives; SELECT/INSERT on events and sequence usage. Workers do not write notes, submit child runs or reconcile tools directly, and have no access to the working-state ledger.

**Rollback permission condition:** the old monorepo Worker readiness probe reads all Runtime tables, including documents, note lifecycle tables, instances, business_runs and app_runs. Retain those historical SELECT grants while that executable is a rollback target. The fresh minimum Worker account is for the new executable; it is not proof that the old binary can start. The existing Windows account's broader grants are retained, not declared retired.

### Explicit legacy access retirement candidate

`retire-access --role <runtime-role> --worker-role <worker-role> --foreign-roles <comma-separated-existing-roles>` uses the administrator's protected `MIGRATION_DATABASE_URL`. This is an explicit, separately coordinated contraction job, never a normal startup or `migrate` side effect. Do not run it in the formal deployment until start/update/rollback/pending-recovery guards verify compatible consumers against the actual bound database. The existing data/HTTP protocol identifiers alone do not prove SQL permission compatibility. Restoring broad grants is not an automatic rollback strategy.

The `runtime-access-v1` transaction touches only the 15 Runtime tables and two sequences. It revokes Worker and explicitly selected foreign roles' direct privileges with `RESTRICT`, grants the minimum Worker profile above, and checks effective table, column, sequence and grant-option permissions before committing. Foreign-domain tables, schema grants and login configuration are preserved. Runtime's own permissions are not reduced. Selected service identities must be existing, distinct leaf roles without superuser, CREATEROLE, CREATEDB, BYPASSRLS, REPLICATION or public-schema creation rights; Runtime objects need a separate migration owner. Active selected service sessions/prepared transactions, dangerous default ACLs, reachable inherited/SET ROLE grants, PUBLIC privileges or dependent grants cause refusal rather than cross-domain cascade.

The coordinator must first verify a restorable backup, prevent new consumer connections, stop consumers and check binary/database bindings. The transaction's session check is not a cross-host fence. If the CLI does not confirm commit, keep consumers stopped and inspect actual ACLs read-only before retrying; loss of a COMMIT reply cannot be classified as rollback from the client error alone. A successful repeated job is idempotent. Normal migration preserves legacy grants before contraction and does not restore them afterward.

Local evidence `runtime-access-retirement-99de620b214440499ff4b38b1a683271` records 23 passing tests/subtests, zero skips, vet, all four Windows executable builds and the execution-package boundary check. Real restricted LOGIN sessions exercised claim/model persistence, heartbeat/save, atomic note/receipt replay and denied access; original notes and foreign-domain privileges were retained. Fifteen refusal cases include partial-REVOKE failure rollback, PUBLIC/column access, INHERIT/SET ROLE, default ACLs, grant dependencies, live connections and replication identities. CLI testing verifies a disposable database even when its original URL contains database overrides, then repeats after the caller discards successful output. It does not simulate losing a PostgreSQL COMMIT packet. Fixture database/role inventories match before/after and the fixture cluster was stopped. Independent review was read-only; formal ACL contraction, release guards and the updated Docker image remain unverified. No paid model calls or Actions ran.

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
one after its caller verifies a separately pinned runner. The separate Worker
candidate now verifies its compiled artifact pin before selecting this engine.
Normal submission can now select the candidate through the deployment option
below. Deployed activation remains a separate gate.

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

`DEFAULT_RUN_ENGINE` defaults to `dsh-0.2.0-rc.2-evidence-v1`; the only alternative
for new root submissions is `dsh-source-639ed015-state-v1`. Unknown values fail
startup. Configure once before serving traffic. Both control and published-app
submissions use this setting; client bodies and Agent definitions cannot select
an engine. The setting is not part of the user-input idempotency hash: a retry
after a deployment change returns the original run and engine. Children inherit
their parent's engine even after the default changes. This accepts configuration,
not proof of compatible Workers; deployment guards must verify migration and
pinned source-enabled Workers before opting in. No live configuration changed.

Control-only `GET /v1/runs/{id}/source-recovery` returns an advisory eligibility
snapshot and machine-readable reason. The action below locks the run and reads
the same conditions again, so cancellation after preview still prevents resume.
The endpoint does not disclose this management view to ToolHub or Knowledge.

Local Windows regression `p25-working-state-ace1fc49c68b4f4cb59660a659eac3c6`
passed 63 test records, vet/build, with five explicitly opt-in binary tests
skipped. New authenticated HTTP submissions cover control and business routes,
both deployment directions, changed-input refusal, client engine refusal and
child inheritance. Subsequent `p25-worker-source-1cc8045977194d6ba13008ac506817cf`
used the complete relocated Worker package and passed all three actual process/
reply-loss scenarios plus recovery boundaries (20 integration pass records,
zero skips). It additionally refuses resume after a previously allowed preview
is invalidated by cancellation. Models, catalog/ToolHub routing and Storage in
these tests are synthetic; PostgreSQL, Runtime, Worker and Node are real.

The control-only `POST /v1/runs/{id}/resume-source-state` resolves exhausted
source-state delivery retries. Under the run lock it requires the source engine,
`waiting_unknown`, no cancellation, the **latest overall** exchange to be a
state read/update with its own acknowledged archive, and no inflight/unknown
calls of any kind. It atomically requeues and records an event without changing
the frozen request, rule revision or response. Generic failed-run resume and
tool reconciliation are unchanged. Source-state unknowns with no unresolved
external calls can also be cancelled without replaying an update; active children
retain the existing waiting-children convergence. These management additions are
newer than published module tag `v0.1.0-rc.2`.

Windows proof `p25-worker-source-167e0a2a71f948a7a324599c3b23816d` passed three
actual Worker/Node source-engine scenarios: kill after state commit before reply,
lost HTTP reply with Worker alive, and three dropped replies followed by explicit
control recovery from `waiting_unknown`. Each used real PostgreSQL and Runtime,
one actual Compact, 69 confirmed archive batches and 475,098 event bytes. Frozen
state retained the dated observation omitted by the synthetic summary. No model,
tool or state effect repeated. Eighteen integration pass records include recovery
and cancellation boundary subtests; none skipped. Model, ToolHub routing and
immutable Storage were fixtures; no paid provider or live installation changed.
The first boundary run `e44de405...` failed because its synthetic unconfirmed
archive violated a table CHECK; the corrected fixture uses a legal staging row.
Its failure log remains retained. A later focused test also checks cancellation
with an active child; final regression evidence is recorded in integration.

Actual disposable PostgreSQL 18 tests cover concurrent claims/fencing, unknown effects, child cancellation, constraint changes, family model budgets, note revision/retirement and audit replay, partial-DDL/GRANT rollback, restricted Runtime/Worker roles and foreign-domain denial. The public facade test injects a lost archive acknowledgement and an exchange-response failure after a note effect committed, then advances the fence and replays without another effect. Storage/effect callbacks in that test are explicitly synthetic; real service/binary handoff is a separate integration gate.

Local full run `p2-runtime-source-f39048c527b64269ba7eddcf057501d0` passed tests/vet/all three builds, with no skipped tests or model calls. Subsequent governance-route tests are tracked in the integration evidence ledger. The first run `9a9dd3b...` failed because old test HTTP configs omitted the newly explicit policy and a concurrency observer waited for a fourth connection in a three-slot fixture pool. Tests now pass policy and observe on the lock-holder connection under a timeout. The second run `3c288cd...` found a test comparing JSONB bytes with unformatted JSON; it now compares JSON values. Original failure reports remain retained.

Independent clone of fixed module `03bee516` passed real PostgreSQL, vet and all three builds with an empty module cache, GOWORK/GOPROXY off and committed vendor (`p2-runtime-source-251793afc8d7478b88b8232c12668eca`). Its [Linux/Windows CI](https://github.com/euler-gaozl/agent-os-runtime/actions/runs/38268513538) passed. Independent static review found no extraction-induced ordering change; it did not execute tests.

The opt-in `TestWorkerBinaryCompatibilityAfterCommittedEffect` takes absolute `TEST_OLD_WORKER_BINARY`, `TEST_NEW_WORKER_BINARY`, `TEST_OLD_DSH_RUNNER`, `TEST_NEW_DSH_RUNNER`, `AGENTOS_NODE` and a disposable `TEST_DATABASE_URL`. It creates its own database and restricted roles, retains the documented legacy readiness reads, verifies actual connection identity, and kills the first executable after Runtime commits a note/receipt but before the proxy returns the reply. Replacement is paused while running at fence 2 to require the exact stale-fence rejection before completion. All three historical engines and both directions passed on Windows with old integration `5bfc84c9`, new Worker `b07b739` and fixed DSH: `p2-worker-binary-handoff-3b30a659311644a9aaa476619ea0f654`. Each case retained one note, one tool HTTP and two model HTTP requests. The temporary cluster stopped afterward; no paid model calls.

Runtime HTTP and PostgreSQL transactions in this test are real; model, ToolHub proxy and immutable Storage HTTP are fixtures. It does not prove complete deployed service integration or Node process-tree cleanup. The earlier `7b43e83...` proof's stale-fence assertion was too weak because it ran after completion; the later proof requires an active replacement lease and exact ErrFence. Live Windows activation remains a separate gate. Passing source or handoff tests does not mean full Agent platform delivery.

Archive retention coverage now lives in `runtime/archive_retention_test.go`: actual SDK HTTP and restricted Runtime/Worker roles cover a lost manifest reply after byte acceptance, original Unicode segments and provenance across takeover, takeover during upload before final acknowledgement, cancellation before acknowledgement, final failed-turn evidence and unsupported-engine exclusion. Five tests passed on disposable PostgreSQL (`p2-runtime-archive-5a53f17d08a24ac48b4f78c874061621`). Storage bytes are an explicit in-memory HTTP fixture; this is not a Storage disk-durability result. Independent review found the initial takeover assertion rejected too early, so the final test changes ownership inside the upload response boundary.

The opt-in `TestIndependentWorkerEvidencePastFourMiBRecallsOmittedSourceAfterProcessInterruption` uses absolute `TEST_NEW_WORKER_BINARY`, `TEST_NEW_DSH_RUNNER`, `AGENTOS_NODE` and disposable `TEST_DATABASE_URL`. It kills the actual independent Worker after Runtime commits a recall receipt but before the HTTP reply, then starts a replacement under the restricted Worker login. Windows proof `p2-worker-evidence-54cef5d09d3f4caabe0b0bd3804351b3` retained 18,874,181 event bytes in 40 batches through 16 DSH compactions, verified the source was omitted from the model context, rediscovered the receipt and reread the original after takeover at fence 2. No committed provider/recall work repeated. The fixture cluster stopped; paid provider calls were zero. This preserves the old scenario under independent processes but does not establish bounded Node memory or complete child-process cleanup. The first attempt failed because its test configuration omitted the Worker's own service identity; the failure record remains retained.

The original proof's cleanup read a mutated configuration map and overwrote the replacement process log. The helper now captures the instance identity before starting the process. Targeted Windows rerun `p2-worker-evidence-ecfcca442b6d46548ac29750099b4e77` passed with no skips, preserved the same recovery assertions, and retained separate nonempty first/replacement logs with hashes. The local runner verifies both logs and records both test-source hashes. This fixes diagnostic provenance; it does not add a new product capability or claim complete Node process-tree cleanup.
