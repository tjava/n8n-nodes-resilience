# n8n-nodes-resilience

Production-grade resilience utilities for n8n workflows.

This community node package helps workflow builders make unreliable integrations easier to manage. The Resilience node inspects incoming item data, adds structured metadata, and routes items so the rest of the workflow can handle retries, concurrency limits, and dead-letter handling with regular n8n nodes.

## Installation

Follow the [n8n community node installation guide](https://docs.n8n.io/integrations/community-nodes/installation/), or install the package directly:

```bash
npm install n8n-nodes-resilience
```

## Node

This package registers one regular n8n node: **Resilience**.

Resources and operations:

- **Retry Strategy**: Evaluate
- **Concurrency Gate**: Acquire, Release, Inspect, Force Release
- **Dead Letter Queue**: Format Failed Item

## Retry Strategy

Use the **Retry Strategy** resource after a risky operation such as an HTTP Request node.

```text
HTTP Request
  -> Resilience (Retry Strategy: Evaluate)
  -> output 1 Success / Continue
  -> output 2 Retry -> Wait -> loop back to HTTP Request
  -> output 3 Exhausted -> Resilience (Dead Letter Queue: Format Failed Item)
```

Retry Strategy does not sleep internally. It calculates `__retry.delaySeconds` and routes the item. Use an n8n Wait node on the Retry output to perform the actual delay before looping back to the risky operation.

Metadata added to each item:

```json
{
  "__retry": {
    "shouldRetry": true,
    "attempt": 1,
    "maxAttempts": 3,
    "strategy": "exponential",
    "delaySeconds": 5,
    "reason": "Status code 503 is retryable",
    "exhausted": false
  }
}
```

Default retryable status codes are `408,429,500,502,503,504`. Default permanent stop codes are `400,401,403,404`.

## Concurrency Gate

Use the **Concurrency Gate** resource to protect a section that should only have a limited number of active items/executions.

```text
Resilience (Concurrency Gate: Acquire)
  -> Acquired / Allowed -> protected work
  -> Blocked -> wait, retry, or skip

protected work -> Resilience (Concurrency Gate: Release)
```

Acquire creates a lock with a TTL and outputs `__concurrencyGate.lockId`. Release can use that lock ID from `__concurrencyGate.lockId`, or an explicit Lock ID parameter.

Important limitation: Concurrency Gate v1 uses n8n workflow static data as a best-effort lock store. It is useful for simple local and single-worker setups, but it is not a perfect distributed lock in all multi-worker, queue-mode, or horizontally scaled deployments. For strict distributed concurrency, use an external lock service or database-backed workflow design.

Metadata added to each item:

```json
{
  "__concurrencyGate": {
    "key": "customer-123",
    "acquired": true,
    "activeCount": 1,
    "maxConcurrent": 1,
    "reason": "Lock acquired",
    "lockId": "lock_mabc1234_abcd5678",
    "expiresAt": "2026-05-24T12:00:00.000Z"
  }
}
```

## Dead Letter Queue

Use the **Dead Letter Queue** resource at permanent failure paths, especially after Retry Strategy's Exhausted output.

```text
Resilience (Retry Strategy: Exhausted)
  -> Resilience (Dead Letter Queue: Format Failed Item)
  -> Google Sheets / Postgres / Slack / Notion / S3 / Webhook / database node
```

The v1 operation is **Format Failed Item** and the destination mode is Output Only. This is intentional: the node formats a durable dead-letter item, and users can connect any regular n8n destination node for storage, notification, or replay.

Example output:

```json
{
  "deadLetterId": "dlq_mabc1234_abcd5678",
  "timestamp": "2026-05-24T12:00:00.000Z",
  "reason": "Request failed",
  "sourceNode": "Dead Letter Queue",
  "workflowId": "workflow-id",
  "workflowName": "Production workflow",
  "executionId": "12345",
  "originalPayload": {},
  "error": {},
  "retry": {}
}
```

## Example Workflows

### HTTP Retry With Dead Letter

1. HTTP Request calls an external API.
2. Resilience with Retry Strategy evaluates `statusCode` and `message`.
3. Retry output goes to Wait using `{{$json.__retry.delaySeconds}}`.
4. Wait loops back to HTTP Request.
5. Exhausted output goes to Resilience with Dead Letter Queue.
6. The dead-letter output connects to a storage or alerting node.

### Concurrency-Limited Section

1. Resilience with Concurrency Gate Acquire uses key `={{$json.customerId}}`.
2. Acquired output does the protected work.
3. The final node releases `{{$json.__concurrencyGate.lockId}}`.
4. Blocked output waits, retries, or skips based on workflow needs.

## Development

```bash
npm install
npm run build
npm run lint
```

## License

[MIT](LICENSE)
