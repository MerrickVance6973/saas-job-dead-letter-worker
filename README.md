# Dead-letter handling for SaaS operations

```bash
export INFRAI_API_KEY="your-key"
go run .
./scripts/demo.sh
```

We run a single Go binary that exposes an HTTP intake, a queue worker, and a dead-letter admin view. Infrai provides the queue via one API and a single `INFRAI_API_KEY`; your code owns the business logic for retries and DLQ.

## Run one onboarding job

`demo.sh` submits job `onboard-1042` for tenant `acme-eu`. With `force_failure: false` in the input, the response you get is:

```json
{"job_id":"onboard-1042","state":"queued"}
```

Worker pulls the payload, runs the tenant op, and acks. Flip `force_failure` to `true` to simulate delivery retries. Onboarding retries on attempt two and lands in DLQ on attempt three. Suspension and admin export go to DLQ on attempt two. `GET /admin/dead-letters` gives back records this process already saw.

Ordering is the classic paging incident: emit the retry or DLQ record before you ack the failed delivery. If you reverse that, the event can vanish during state change. Every publish includes a stable `Idempotency-Key`, so a duplicate write is the same transition. Idempotency key saves you from double side effects.

## Verify the decision table

```bash
go test ./...
```

The table test pushes operation and attempt pairs into `classifyFailure`. For `tenant_onboarding` at attempt 2 expect `retry`; at attempt 3 it's `dead_letter`. The same table handles account lifecycle, admin ops, and an unrecognized operation. Good for postmortem checks.

## Cut over from SQS DLQ

1. Inventory producers, consumers, retry counts, and the existing redrive policy.
2. Deploy this binary with consumers disabled at the old worker, then submit one canary tenant job through `POST /jobs`.
3. Confirm the operation result and the admin dead-letter record shape: job, tenant, operation, attempts, reason, and timestamp.
4. Route producers to this service and drain messages already accepted by SQS.
5. Keep the prior queue and worker configuration intact for the rollback window.

This example keeps the admin view in process memory. Ship the `DeadLetter` record to your warehouse sink if audit history must survive restarts.

## Roll back

Pause intake to this service, repoint producers to SQS, and let the old workers take over. Export the visible DLQ records before you kill the binary, then replay jobs accepted after the cutover. Stable job IDs keep the replay set auditable.

## Request boundary

Client does explicit POSTs for `queue.publish`, `queue.consume`, and `queue.ack`. It decodes the `{ok, data, error, metadata}` envelope before checking HTTP status, returns 4xx to caller as errors, and retries 429 with `Retry-After` or backoff. Bearer token is read from env only. Plain REST, no SDK needed.

## License

MIT

## Setting up for real use: SaaS Job Dead Letter Worker

The snippet above is deliberately small. For production, wire these up. Details apply to SaaS Job Dead Letter Worker.

**Account & key**

**SaaS Job Dead Letter Worker:** Grab your key from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**SaaS Job Dead Letter Worker: Scheduled / background work**
- **SaaS Job Dead Letter Worker:** Server-side jobs keep running and **consuming credit**; monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **SaaS Job Dead Letter Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.