# Operations runbook

## Health and telemetry

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness: the process is serving. |
| `GET /readyz` | Readiness: the store is reachable. |
| `GET /metrics` | Prometheus metrics (scrape with Managed Service for Prometheus or a sidecar). |

Key metrics:

- `strata_operations_total{kind,result}`: alert on `result="ROLLBACK_FAILED"` or `"DELETE_FAILED"` (needs a human).
- `strata_provider_calls_total{type,action,result="error"}`: cloud API errors by type.
- `strata_provider_call_duration_seconds`: slow APIs (Cloud Run creates are normally 30–90s).
- `strata_operations_running`: work in progress per instance.
- `strata_http_requests_total{code=~"5.."}`: API errors.

Logs are Cloud Logging JSON. Useful filters:

```
resource.type="cloud_run_revision" jsonPayload.message="audit"                         # who did what
resource.type="cloud_run_revision" jsonPayload.message="resource event" severity>=WARNING  # failed steps
resource.type="cloud_run_revision" jsonPayload.operation="op-..."                       # one operation
```

`strata events -s <stack>` shows the same resource timeline from the database.

## Stack statuses

| Status | Meaning | Action |
|---|---|---|
| `READY` | Last deploy succeeded. If `statusReason` mentions pending cleanup, a replaced or removed resource could not be deleted. | Check `strata describe`; the next deploy retries cleanup. |
| `CREATING` / `UPDATING` / `ROLLING_BACK` / `DELETING` | An operation is running. | Wait; `strata events --follow`. |
| `ROLLBACK_COMPLETE` | Deploy failed and was fully reverted. | Read the operation error, fix the template, deploy again. |
| `ROLLBACK_FAILED` | Deploy failed and at least one undo step failed. | See below. |
| `DELETE_FAILED` | Some resources could not be deleted. | Fix the cause (permissions, non-empty bucket without `forceDestroy`), then `strata destroy` again. |

### ROLLBACK_FAILED

1. `strata describe -s <stack> --json` shows resources with `status: FAILED` and `statusReason`, plus `pendingCleanup`.
2. Fix the underlying cause (usually a missing permission or a quota).
3. Deploy again: resources in `FAILED` state are always re-applied, and `pendingCleanup` is retried.
4. If a resource was deleted out of band, deploying recreates it. Drift detection (`strata drift`) shows which.

## Stuck operations

Operations cannot get stuck behind a dead worker: leases expire (`STRATA_LEASE`, default 60s) and another instance resumes. To inspect:

```sql
SELECT id, stack, status, lease_owner, lease_expires, attempts, doc->'checkpoint'->>'phase'
FROM operations WHERE status IN ('PENDING','RUNNING') ORDER BY created_at;
```

An operation with a growing `attempts` count is crashing its worker on resume; check logs for that operation ID.

## Scaling

- Increase `--max-instances` for more throughput; all instances run workers and share the queue.
- Split roles with `STRATA_MODE=api` and a separate `STRATA_MODE=worker` service if API latency matters during heavy deploys. Worker-only instances serve only health and metrics.
- Keep `--no-cpu-throttling` and `--min-instances >= 1`: workers run in the background, not per request.
- Cloud SQL `db-custom-1-3840` handles thousands of stacks; enable `SQL_AVAILABILITY=regional` for HA.

## Upgrades

Migrations run automatically at startup under a Postgres advisory lock, so rolling deploys with several instances are safe. Operations interrupted by a rollout resume on new revisions.

## Backup and restore

Cloud SQL automated backups and point-in-time recovery are enabled by the bootstrap. The database is the source of truth for stack state; restoring an older snapshot means Strata's view may lag reality. Run `strata drift` on affected stacks after a restore, then redeploy to converge.

## Credentials

The engine uses its Cloud Run service account through the metadata server; there are no keys. Its project roles are listed in `deploy/bootstrap.sh`. Remove roles for resource types you do not use.
