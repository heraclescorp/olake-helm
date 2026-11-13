# Contributing to the OLake Helm Chart

Thanks for helping improve the chart. This guide covers how we branch, write commits, open pull requests and document chart values.

For changes to OLake itself, see the [OLake repository](https://github.com/datazip-inc/olake). For documentation on olake.io, see [olake-docs](https://github.com/datazip-inc/olake-docs).

## Prerequisites

- [Helm](https://helm.sh/docs/intro/install/) 3 or later
- `make`, `git`, `curl` and `tar`

`helm-docs` is downloaded automatically when needed (see [Chart values and documentation](#chart-values-and-documentation)).

Run `make` from the repository root to list the available targets.

## Branching

- Branch from `staging` and open your pull request against `staging`. Never target `master`.
- `master` holds released code. Maintainers promote `staging` to `master` through a release pull request.
- Name branches `<type>/<short-description>`, in lowercase kebab-case, using the types below:

| Type | Use for |
|------|---------|
| `feat/` | New chart features or values |
| `fix/` | Bug fixes |
| `chore/` | Maintenance, dependency bumps, releases |
| `docs/` | Documentation only |
| `test/` | Tests only |
| `refactor/` | Code or template changes with no behavior change |
| `ci/` | GitHub Actions workflows and CI scripts |

Examples: `feat/job-pod-labels`, `fix/nfs-storage-class`, `docs/contributing`.

## Commits and pull request titles

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<optional scope>): <short summary in imperative mood>
```

- **Types:** `feat`, `fix`, `chore`, `docs`, `test`, `refactor`, `ci`, the same set as branch names.
- **Scopes** name the area changed, for example `helm`, `worker`, `fusion`, `postgresql`, `ci`.
- **Breaking changes:** add `!` after the type or scope, for example `feat(helm)!: rename olakeWorker.config`, and explain the migration in the pull request description.

Examples:

```
feat(helm): add global.jobPodLabels for activity pods
fix(worker): cross-compile release image
docs: add contributing guide
```

Pull requests are squash merged, so the **pull request title becomes the commit on `staging`**. Make sure it follows the same format.

## Pull requests

- Keep one change per pull request.
- Open a [draft pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests#draft-pull-requests) while the work is in progress, and mark it ready when you want a review.
- Fill in the pull request template, including how you tested the change.
- Sign the Contributor License Agreement when the CLA Assistant asks.

### During review

- Reply to every review comment, either with what you changed or with why you disagree.
- Push follow-up commits to the same branch. Do not force push during review, and do not close the pull request and open a new one. Reviewers need to see what changed since their last review, and the history is squashed on merge anyway.
- Request another review only after every comment has been handled.

## Chart values and documentation

`helm/olake/values.yaml` is the source of truth for chart configuration. The values reference in [`helm/olake/OLAKE_README.md`](./helm/olake/OLAKE_README.md) is generated from its comments by [helm-docs](https://github.com/norwoodj/helm-docs). Do not edit that file by hand.

### Comment format

Document each value with a `# --` comment directly above the key:

```yaml
global:
  # -- Global storage class for all persistent volumes.
  # Set to "" to use the cluster's default storage class.
  storageClass: ""

  # -- Service account settings for job pods created by olake-workers
  # @default -- creates a service account named after the release
  jobServiceAccount:
    create: true
    name: ""
```

- `# --` starts the description. The comment lines that follow are part of the same description.
- Plain `#` comments are ignored. A key without a `# --` comment still appears in the table, but with an empty description.
- Document nested keys with their own `# --` comment.
- Use `# @default -- <text>` when the literal default is unclear, for example an empty value that the chart fills in.
- Descriptions are rendered on a single line in a table, so keep them short. Put long YAML examples in [`helm/olake/README.md`](./helm/olake/README.md) instead.

### When you add or change a value

1. Add or update the `# --` comment in `values.yaml`.
2. For a new top-level key, add it to a category in `helm/olake/OLAKE_README.md.gotmpl`. Otherwise it is listed under "Other Configurations".
3. Run the checks and commit the regenerated `OLAKE_README.md`:

```bash
make lint
make docs
```

`make docs` uses `helm-docs` from your `PATH` if it is the pinned version. Otherwise it downloads that version into `.bin/` and verifies its checksum first.

CI runs `make lint` and `make docs-check` on every pull request that touches the chart or the docs tooling. `docs-check` fails if `OLAKE_README.md` is out of date.

## Testing your changes

Lint the chart and render it with your values before opening a pull request:

```bash
make lint
helm template olake helm/olake -f my-values.yaml
```

For template or behavior changes, install the chart on a local cluster (kind, minikube or k3d) and describe what you checked in the pull request.

### Atomic table resync worker

Deploy the UI database migration before this worker: nullable `execution_workflow_id` and `execution_run_id` on jobs, and `<job table>_execution_records` with its `(job_id, workflow_id)` primary key, `(job_id, phase)` index and internal `controlled BOOLEAN NOT NULL DEFAULT false`. Execution records and closed tombstones survive ownership release and job deletion. A legacy non-null workflow owner with a null run epoch is unresolved ownership, not an idle job or permission to adopt it.

Pause schedules and drain every pre-upgrade sync, clear and resync workflow **and its native connector resources** before replacing the worker. Use the original worker version to reconcile a legacy unresolved owner or replay an old history; never fill in a guessed epoch or clear it blindly. This unversioned workflow change must not replay/reset pre-upgrade histories. Deploy matching connector images and Java writers supporting the positive execution fence, launch-bound receipts, `EXECUTION_CAPABILITY`, and recovery-only mode before enabling requests. Unsupported images fail closed; no fallback to destructive clear is permitted. All controlled sync/clear/resync commands now require this compatible runtime, not only table resync.

The positive runtime fence/admission/settlement protocol below applies only to the canonical atomic-resync domain: saved source type PostgreSQL, saved destination type and config type Iceberg, and saved `writer.catalog_type` in `rest`, `lakekeeper`, `nessie`, `s3tables`, `unity`, `polaris`, `biglake` or `horizon` (case/space normalized). Native S3 Tables is in this REST family. Empty/missing catalog type defaults to Glue and is excluded. The worker derives and persists `controlled` while holding the SQL job fence and canonical source/destination row locks; caller flags cannot enable or downgrade it. Unsupported resync is rejected before launch. Ordinary Glue, parquet, other-source and other-catalog jobs preserve their legacy provider names, artifact layout, retry, cleanup and state lifecycle without execution flags or retirement receipts; their outcome is recorded as legacy policy, never fabricated as `settled`. Ownership/state writes still compare workflow and run. Existing records are not backfilled into controlled mode; a domain change on a retained operation fails closed instead of downgrading it.

`WorkflowID` / `--resync-id` remains the stable operation identity. Temporal supplies the controlling `RunID`; the public API cannot choose it. One SQL decision admits exactly one launcher attempt or closes the operation before launch. Admission chooses a unique runtime/artifact identity and immutable launch ID. Every retry after admission attaches only: a missing or merely-created backend is UNKNOWN, never permission to create/start a second data process. SQL owner and state CAS compare both workflow and run; cleanup first seals admission, acknowledges runtime-specific CLOSED, then resolves/persists and deletes the exact Docker container ID or Kubernetes pod UID before release. New controlling runs must positively describe the exact old Temporal run as terminal and serialize old cleanup before adoption. Admitted sync/clear generations are never relaunched by a reset.

Every controlled asynchronous connector in this domain receives `--execution-id` and `--execution-fence`; resync also receives `--resync-launch-id`. Fence JSON is version 1 with `execution_id` and `state: "open"|"closed"`. OPEN uses storage create-if-absent; CLOSED is an acknowledged atomic complete-file replacement. Missing, unreadable, malformed, mismatched or CLOSED is never permission to mutate. Filesystem storage must support hard links, atomic rename and directory fsync; S3-compatible services must honor `If-None-Match: *`. Do not delete or edit reserved execution fence/control artifacts, including after job deletion: a delayed old OPEN could otherwise recreate a removed CLOSED identity. Worker log retention deletes logs only in controlled directories; fences, configs, proofs and immutable runtime pointers are retained. There is no automatic tombstone expiry.

Resync loads the full saved source, destination, catalog and checkpoint; automatic enrollment does not run. A success receipt must match the stable operation/target, original `launch_id`, current runtime `execution_id`, table UUID/snapshot and SHA-256 of the complete final JSON-object checkpoint (large integers are preserved). Only the retired matching runtime's `not_published` abort proof permits an admitted failed operation to release ownership. Worker preflight errors never synthesize connector abort evidence: close-before-launch is proved only by the SQL admission state. Missing/stale/unknown proof retains ownership and a paused schedule.

Within this domain, ordinary sync and clear also require a launch-bound `execution-result.json`: version 1, exact `execution_id`, `launch_id`, command, and `outcome: "not_started"|"settled"|"unknown"`. `not_started` preserves the saved database checkpoint without reading candidate state. `settled` binds the complete final checkpoint with `state_sha256` and proves all dispatched mutations were definitively answered and the JVM's mutation admission was terminally sealed; it does **not** mean the command succeeded. A source authentication/setup failure before destination initialization can prove `not_started`; a local TOAST/extraction failure can prove `settled` after earlier destination work is acknowledged. Exit code 0 or 1, JVM disappearance or native-resource absence is never this proof. Missing, ambiguous or `unknown` outcomes retain ownership and pause the supported job's schedule, even when the backend exited successfully.

For a terminal ordinary sync/clear workflow whose cleanup did not finish, reset the **same WorkflowID with its original command** from before the original sync/clear activity. The new run first validates and retires the old run's exact runtime using that receipt, CLOSED fence and immutable resource identity, and persists only a receipt-bound checkpoint. It adopts the operation as CLOSED/retired, never admits another data launcher, and reports `PriorExecutionRetired`; its disconnected cleanup/release then frees the job. Start a fresh ordinary sync or a fresh UI table-resync operation afterward. This is the supported bad-password/TOAST-to-resync recovery path, not a SQL owner reset. Unknown/missing receipts cannot use it and remain owned and mutation-fenced until real source/catalog evidence is available. A never-admitted old run closes before launch and is also adopted as retired, never permission for a reset to launch the same data operation.

The API/UI's authenticated `GET`/`POST /api/v1/project/:projectid/jobs/:id/reconcile` observes the SQL owner and resets only its same terminal Temporal operation. Cancellation is a request, not retirement evidence. Before admitting any resync recovery runtime, the worker checks the original launch's exact `not_published` receipt after closing its fence and confirming deletion of its immutable resource. Positive non-publication closes/adopts the operation as retired without launching a recovery connector; absent, stale or unknown evidence never authorizes that shortcut. Cancellation during an already-bound handoff cannot restore an unbound schedule or discard its transfer. Successful retirement leaves the ordinary schedule paused; the UI offers a separate explicit Resume schedule action, guarded by no owner/no handoff and a positively identified normal sync action. An accepted reset is not a completed reconciliation. Old ordinary `unknown` receipts are never rewritten or reclassified from logs; independently established remote-outcome evidence and a connector-supported reconciliation mechanism remain necessary.

For an unknown outcome, keep the job paused and retain its owner. Inspect the durable execution row for controlling run, original launch, current runtime and immutable native resource. If that Temporal run is still running, do not reset it: deploy a compatible worker on the same task queue and let its existing attach/cleanup activity retry. A published run already at `closed`/`published` retries checkpoint handoff without another source launch; confirm the existing activity and final handoff in Temporal before any new operation. Workflow-history changes still require the drain rule above; the JSONB handoff query correction does not change workflow commands. To recover a terminal run, reset **the same WorkflowID** from before `ResyncActivity`; the new Temporal RunID retires the old runtime and admits a new, uniquely named `--resync-recover-only` runtime with the original launch ID. This mode can only reconcile an existing matching durable publication and restore its already-committed checkpoint; it cannot stage, publish replacement data, or infer unpublished from marker absence. Unknown/missing publication still retains ownership. If create acknowledgement was lost and no immutable resource was recorded, a missing provider resource remains UNKNOWN: allow the original request to settle and retry retirement once its runtime can be positively identified. A recorded immutable resource that is confirmed absent is safe to retire idempotently. Never bypass an unresolved resource/catalog result by manually clearing ownership, deleting the control objects, changing IDs, or launching a replacement operation.

Successful publication persists a durable HANDOFF bound to the checkpoint hash, catalog hash, active intent and internal transfer token. Ownership remains held while restoring the normal action and sending a stable-ID Temporal patch. A lost patch acknowledgement can be completed either by retrying that exact patch or by the token-bearing sync atomically accepting the matching transfer. A reset during pending handoff preserves its original transfer epoch/token/request ID and resumes handoff without launching any source process. Old retries after handoff has been consumed never unpause or trigger again. Queued ordinary sync/resync claims recheck current `active` intent under the SQL fence; explicit clear remains supported for paused jobs.

Stable worker logs remain under the WorkflowID directory; connector configs, state, proof and logs use sibling runtime-specific SHA-256 directories. `execution-runtime.json` and immutable per-runtime pointers describe them; UI readers use the SQL execution record as current runtime authority so delayed old pointer writes cannot hide recovery logs. Keep original launch logs and recovery artifacts when diagnosing unknown outcomes. Separate jobs sharing a destination table or replication slot, and standalone source/destination edits during an operation, remain unsupported.

Prepared behavior checks: from `worker`, run `go test -race ./storage ./utils ./types ./executor ./executor/docker ./executor/kubernetes ./temporal ./database`. Set `OLAKE_ENROLLMENT_TEST_DSN` to an isolated localhost PostgreSQL URL for actual SQL admission/closure, stale epoch, paused intent, canonical-domain policy and durable handoff interleavings; the test creates/removes its own schema. Run these only after all coordinated repository changes have settled.

