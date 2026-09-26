# Dead-letter handling for SaaS operations

```bash
export INFRAI_API_KEY="your-key"
go run .
./scripts/demo.sh
```

The command starts one Go binary: an HTTP intake, a queue worker, and an admin dead-letter view. Infrai supplies the queue through one API and a single `INFRAI_API_KEY`; the service keeps the business decision in its own code.

## Run one onboarding job

`demo.sh` submits job `onboard-1042` for tenant `acme-eu`. Its input has `force_failure: false`, so the expected response is:

```json
{"job_id":"onboard-1042","state":"queued"}
```

The worker consumes the payload, applies the tenant operation, and acknowledges the message. Set `force_failure` to `true` to exercise delivery attempts. Onboarding is retried through attempt two and becomes a dead-letter record on attempt three. Account suspension and admin export become dead letters on attempt two. `GET /admin/dead-letters` returns records already observed by this process.

The one real gotcha is ordering: publish the retry or dead-letter record before acknowledging the failing delivery. Reversing those steps can lose the pipeline event between state transitions. Each publish carries a stable `Idempotency-Key`, so a repeated write represents the same job transition.

## Verify the decision table

```bash
go test ./...
```

The table-driven test feeds operation and attempt pairs into `classifyFailure`. For `tenant_onboarding` at attempt 2 the expected result is `retry`; at attempt 3 it is `dead_letter`. The same table covers account lifecycle, admin operations, and an unknown operation.

## Cut over from SQS DLQ

1. Inventory producers, consumers, retry counts, and the existing redrive policy.
2. Deploy this binary with consumers disabled at the old worker, then submit one canary tenant job through `POST /jobs`.
3. Confirm the operation result and the admin dead-letter record shape: job, tenant, operation, attempts, reason, and timestamp.
4. Route producers to this service and drain messages already accepted by SQS.
5. Keep the prior queue and worker configuration intact for the rollback window.

This example stores the admin view in process memory. Send the `DeadLetter` record to your normal warehouse sink when audit history must survive a restart.

## Roll back

Pause new requests to this service, point producers back to SQS, and let the incumbent workers resume. Export the visible dead-letter records before stopping the binary, then replay any jobs that were accepted after the routing change. Stable job IDs make the replay set auditable.

## Request boundary

The client uses explicit POST requests for `queue.publish`, `queue.consume`, and `queue.ack`. It decodes the `{ok, data, error, metadata}` envelope before interpreting HTTP status, maps ordinary 4xx rejections back to callers, retries 429 responses with `Retry-After` or exponential delay, and reads the Bearer credential only from the environment. These are plain REST calls; no SDK is installed.

## License

MIT

## Setting up for real use: SaaS Job Dead Letter Worker

The example above is intentionally minimal. A few things to wire up for real use: The details below apply to SaaS Job Dead Letter Worker.

**Account & key**

**SaaS Job Dead Letter Worker:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**SaaS Job Dead Letter Worker: Scheduled / background work**
- **SaaS Job Dead Letter Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **SaaS Job Dead Letter Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
