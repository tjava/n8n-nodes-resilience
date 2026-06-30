import {
  IDataObject,
  IExecuteFunctions,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
  NodeConnectionTypes,
  NodeOperationError,
} from "n8n-workflow";

import { getValueAtPath } from "../shared/fieldPath";
import { calculateDelaySeconds, RetryStrategyName } from "../shared/retry";
import {
  cleanExpiredLocks,
  ConcurrencyLock,
  getConcurrencyState,
  getConcurrencyStates,
} from "../shared/staticData";
import {
  asDataObject,
  createOutputItem,
  deepCopy,
  generateId,
  isoFromTimestamp,
  parseNumberList,
  removeUndefinedValues,
  toNumber,
  toStringValue,
} from "../shared/utils";

type ResilienceResource =
  | "retryStrategy"
  | "concurrencyGate"
  | "deadLetterQueue";
type ConcurrencyOperation = "acquire" | "release" | "inspect" | "forceRelease";
type LimitMode = "routeBlocked" | "allowWithMetadata";

export class Resilience implements INodeType {
  description: INodeTypeDescription = {
    displayName: "Resilience",
    name: "resilience",
    icon: {
      light: "file:resilience.svg",
      dark: "file:resilience.dark.svg",
    },
    group: ["transform"],
    version: 1,
    subtitle: '={{$parameter["resource"] + ": " + $parameter["operation"]}}',
    description: "Retry, concurrency, and dead-letter utilities for workflows",
    defaults: {
      name: "Resilience",
    },
    inputs: [NodeConnectionTypes.Main],
    outputs: [
      {
        type: NodeConnectionTypes.Main,
        displayName: "Success / Allowed / Dead Letter",
      },
      {
        type: NodeConnectionTypes.Main,
        displayName: "Retry / Blocked",
      },
      {
        type: NodeConnectionTypes.Main,
        displayName: "Exhausted",
      },
    ],
    usableAsTool: true,
    outputNames: [
      "Success / Allowed / Dead Letter",
      "Retry / Blocked",
      "Exhausted",
    ],
    properties: [
      {
        displayName: "Resource",
        name: "resource",
        type: "options",
        noDataExpression: true,
        options: [
          {
            name: "Concurrency Gate",
            value: "concurrencyGate",
            description: "Limit active items in a protected workflow section",
          },
          {
            name: "Dead Letter Queue",
            value: "deadLetterQueue",
            description: "Format failed items for storage or replay",
          },
          {
            name: "Retry Strategy",
            value: "retryStrategy",
            description: "Evaluate whether an item should retry or fail",
          },
        ],
        default: "retryStrategy",
      },
      {
        displayName: "Operation",
        name: "operation",
        type: "options",
        noDataExpression: true,
        displayOptions: {
          show: {
            resource: ["retryStrategy"],
          },
        },
        options: [
          {
            name: "Evaluate",
            value: "evaluate",
            description: "Evaluate the retry policy for each input item",
            action: "Evaluate retry strategy",
          },
        ],
        default: "evaluate",
      },
      {
        displayName: "Operation",
        name: "operation",
        type: "options",
        noDataExpression: true,
        displayOptions: {
          show: {
            resource: ["concurrencyGate"],
          },
        },
        options: [
          {
            name: "Acquire",
            value: "acquire",
            description: "Try to acquire a lock for this gate key",
            action: "Acquire concurrency lock",
          },
          {
            name: "Force Release",
            value: "forceRelease",
            description: "Clear all active locks for this gate key",
            action: "Force release concurrency locks",
          },
          {
            name: "Inspect",
            value: "inspect",
            description: "Return current lock metadata for this gate key",
            action: "Inspect concurrency gate",
          },
          {
            name: "Release",
            value: "release",
            description: "Release a previously acquired lock",
            action: "Release concurrency lock",
          },
        ],
        default: "acquire",
      },
      {
        displayName: "Operation",
        name: "operation",
        type: "options",
        noDataExpression: true,
        displayOptions: {
          show: {
            resource: ["deadLetterQueue"],
          },
        },
        options: [
          {
            name: "Format Failed Item",
            value: "formatFailedItem",
            description: "Wrap the input item as a dead-letter payload",
            action: "Format failed item",
          },
        ],
        default: "formatFailedItem",
      },
      {
        displayName: "Max Attempts",
        name: "maxAttempts",
        type: "number",
        typeOptions: {
          minValue: 1,
        },
        displayOptions: {
          show: {
            resource: ["retryStrategy"],
          },
        },
        default: 3,
        description: "Maximum retry attempts before the item is exhausted",
      },
      {
        displayName: "Strategy",
        name: "strategy",
        type: "options",
        noDataExpression: true,
        displayOptions: {
          show: {
            resource: ["retryStrategy"],
          },
        },
        options: [
          {
            name: "Exponential",
            value: "exponential",
            description: "Double the delay after each attempt",
            action: "Use exponential retry delay",
          },
          {
            name: "Fixed",
            value: "fixed",
            description: "Use the same delay for every retry",
            action: "Use fixed retry delay",
          },
          {
            name: "Linear",
            value: "linear",
            description: "Increase delay by the base amount on each attempt",
            action: "Use linear retry delay",
          },
        ],
        default: "exponential",
        description: "How to calculate the retry delay",
      },
      {
        displayName: "Base Delay Seconds",
        name: "baseDelaySeconds",
        type: "number",
        typeOptions: {
          minValue: 0,
        },
        displayOptions: {
          show: {
            resource: ["retryStrategy"],
          },
        },
        default: 5,
        description: "Base delay used by the selected retry strategy",
      },
      {
        displayName: "Max Delay Seconds",
        name: "maxDelaySeconds",
        type: "number",
        typeOptions: {
          minValue: 0,
        },
        displayOptions: {
          show: {
            resource: ["retryStrategy"],
          },
        },
        default: 300,
        description:
          "Maximum delay to output after strategy and jitter are applied",
      },
      {
        displayName: "Enable Jitter",
        name: "enableJitter",
        type: "boolean",
        displayOptions: {
          show: {
            resource: ["retryStrategy"],
          },
        },
        default: true,
        description:
          "Whether to randomize the delay to reduce synchronized retries",
      },
      {
        displayName: "Jitter Percentage",
        name: "jitterPercentage",
        type: "number",
        typeOptions: {
          minValue: 0,
        },
        displayOptions: {
          show: {
            resource: ["retryStrategy"],
            enableJitter: [true],
          },
        },
        default: 30,
        description:
          "Percentage range to randomize around the calculated delay",
      },
      {
        displayName: "Retry Status Codes",
        name: "retryStatusCodes",
        type: "string",
        displayOptions: {
          show: {
            resource: ["retryStrategy"],
          },
        },
        default: "408,429,500,502,503,504",
        description: "Comma-separated HTTP status codes that should be retried",
      },
      {
        displayName: "Stop Status Codes",
        name: "stopStatusCodes",
        type: "string",
        displayOptions: {
          show: {
            resource: ["retryStrategy"],
          },
        },
        default: "400,401,403,404",
        description:
          "Comma-separated HTTP status codes that should fail permanently",
      },
      {
        displayName: "Status Code Field",
        name: "statusCodeField",
        type: "string",
        displayOptions: {
          show: {
            resource: ["retryStrategy"],
          },
        },
        default: "statusCode",
        description: "Dot path containing the status code on each input item",
      },
      {
        displayName: "Error Message Field",
        name: "errorMessageField",
        type: "string",
        displayOptions: {
          show: {
            resource: ["retryStrategy"],
          },
        },
        default: "message",
        description: "Dot path containing the error message on each input item",
      },
      {
        displayName: "Attempt Field",
        name: "attemptField",
        type: "string",
        displayOptions: {
          show: {
            resource: ["retryStrategy"],
          },
        },
        default: "__retry.attempt",
        description:
          "Dot path containing the last retry attempt count on each input item",
      },
      {
        displayName: "Gate Key",
        name: "gateKey",
        type: "string",
        required: true,
        displayOptions: {
          show: {
            resource: ["concurrencyGate"],
          },
        },
        default: "",
        placeholder: "={{$json.customerId || 'global'}}",
        description:
          "Unique key for the protected workflow section, such as a customer ID or external service name",
      },
      {
        displayName: "Max Concurrent",
        name: "maxConcurrent",
        type: "number",
        typeOptions: {
          minValue: 1,
        },
        displayOptions: {
          show: {
            resource: ["concurrencyGate"],
            operation: ["acquire", "inspect"],
          },
        },
        default: 1,
        description: "Maximum active locks allowed for this gate key",
      },
      {
        displayName: "Lock TTL Seconds",
        name: "lockTtlSeconds",
        type: "number",
        typeOptions: {
          minValue: 1,
        },
        displayOptions: {
          show: {
            resource: ["concurrencyGate"],
            operation: ["acquire", "inspect"],
          },
        },
        default: 300,
        description: "How long an acquired lock remains active if not released",
      },
      {
        displayName: "On Limit Reached",
        name: "onLimitReached",
        type: "options",
        noDataExpression: true,
        displayOptions: {
          show: {
            resource: ["concurrencyGate"],
            operation: ["acquire"],
          },
        },
        options: [
          {
            name: "Allow With Metadata Only",
            value: "allowWithMetadata",
            description:
              "Allow items through but mark that the lock was not acquired",
            action: "Allow without acquiring",
          },
          {
            name: "Route to Blocked Output",
            value: "routeBlocked",
            description:
              "Send items over the blocked output when the gate is full",
            action: "Route blocked items",
          },
        ],
        default: "routeBlocked",
        description: "What to do when the active lock count is at the limit",
      },
      {
        displayName: "Lock ID Field",
        name: "lockIdField",
        type: "string",
        displayOptions: {
          show: {
            resource: ["concurrencyGate"],
            operation: ["release"],
          },
        },
        default: "__concurrencyGate.lockId",
        description:
          "Dot path containing the lock ID to release. Leave blank to use the metadata lock ID.",
      },
      {
        displayName: "Lock ID",
        name: "lockId",
        type: "string",
        displayOptions: {
          show: {
            resource: ["concurrencyGate"],
            operation: ["release"],
          },
        },
        default: "",
        description:
          "Optional explicit lock ID to release. Takes precedence over Lock ID Field.",
      },
      {
        displayName: "Failure Reason Field",
        name: "failureReasonField",
        type: "string",
        displayOptions: {
          show: {
            resource: ["deadLetterQueue"],
          },
        },
        default: "message",
        description:
          "Dot path containing the failure reason. Falls back to error metadata when empty.",
      },
      {
        displayName: "Source Node Field",
        name: "sourceNodeField",
        type: "string",
        displayOptions: {
          show: {
            resource: ["deadLetterQueue"],
          },
        },
        default: "sourceNode",
        description: "Optional dot path containing the source node name",
      },
      {
        displayName: "Workflow ID Field",
        name: "workflowIdField",
        type: "string",
        displayOptions: {
          show: {
            resource: ["deadLetterQueue"],
          },
        },
        default: "workflowId",
        description: "Optional dot path containing a workflow ID override",
      },
      {
        displayName: "Workflow Name Field",
        name: "workflowNameField",
        type: "string",
        displayOptions: {
          show: {
            resource: ["deadLetterQueue"],
          },
        },
        default: "workflowName",
        description: "Optional dot path containing a workflow name override",
      },
      {
        displayName: "Execution ID Field",
        name: "executionIdField",
        type: "string",
        displayOptions: {
          show: {
            resource: ["deadLetterQueue"],
          },
        },
        default: "executionId",
        description: "Optional dot path containing an execution ID override",
      },
      {
        displayName: "Include Original Payload",
        name: "includeOriginalPayload",
        type: "boolean",
        displayOptions: {
          show: {
            resource: ["deadLetterQueue"],
          },
        },
        default: true,
        description: "Whether to include the incoming JSON as originalPayload",
      },
      {
        displayName: "Include Error Metadata",
        name: "includeErrorMetadata",
        type: "boolean",
        displayOptions: {
          show: {
            resource: ["deadLetterQueue"],
          },
        },
        default: true,
        description: "Whether to include error, retry, and gate metadata",
      },
      {
        displayName: "Destination Mode",
        name: "destinationMode",
        type: "options",
        noDataExpression: true,
        displayOptions: {
          show: {
            resource: ["deadLetterQueue"],
          },
        },
        options: [
          {
            name: "Output Only",
            value: "outputOnly",
            description: "Send the dead-letter item to the node output",
            action: "Output dead-letter item",
          },
        ],
        default: "outputOnly",
        description:
          "External dead-letter storage is intentionally left to downstream n8n nodes",
      },
      {
        displayName: "Preserve Original Item",
        name: "preserveOriginalItem",
        type: "boolean",
        displayOptions: {
          hide: {
            resource: ["deadLetterQueue"],
          },
        },
        default: true,
        description:
          "Whether to keep the incoming JSON and binary data when adding metadata",
      },
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    try {
      const resource = this.getNodeParameter(
        "resource",
        0,
      ) as ResilienceResource;

      if (resource === "retryStrategy") {
        return executeRetryStrategy.call(this);
      }

      if (resource === "concurrencyGate") {
        return executeConcurrencyGate.call(this);
      }

      return executeDeadLetterQueue.call(this);
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unknown resilience error";

      if (this.continueOnFail()) {
        return [
          [{ json: { error: message }, pairedItem: { item: 0 } }],
          [],
          [],
        ];
      }

      throw new NodeOperationError(this.getNode(), message, { itemIndex: 0 });
    }
  }
}

async function executeRetryStrategy(
  this: IExecuteFunctions,
): Promise<INodeExecutionData[][]> {
  const items = this.getInputData();
  const successItems: INodeExecutionData[] = [];
  const retryItems: INodeExecutionData[] = [];
  const exhaustedItems: INodeExecutionData[] = [];

  for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
    try {
      const item = items[itemIndex];
      const maxAttempts = Math.max(
        1,
        this.getNodeParameter("maxAttempts", itemIndex) as number,
      );
      const strategy = this.getNodeParameter(
        "strategy",
        itemIndex,
      ) as RetryStrategyName;
      const baseDelaySeconds = this.getNodeParameter(
        "baseDelaySeconds",
        itemIndex,
      ) as number;
      const maxDelaySeconds = this.getNodeParameter(
        "maxDelaySeconds",
        itemIndex,
      ) as number;
      const enableJitter = this.getNodeParameter(
        "enableJitter",
        itemIndex,
      ) as boolean;
      const jitterPercentage = enableJitter
        ? (this.getNodeParameter("jitterPercentage", itemIndex) as number)
        : 0;
      const retryStatusCodes = parseNumberList(
        this.getNodeParameter("retryStatusCodes", itemIndex) as string,
      );
      const stopStatusCodes = parseNumberList(
        this.getNodeParameter("stopStatusCodes", itemIndex) as string,
      );
      const statusCodeField = this.getNodeParameter(
        "statusCodeField",
        itemIndex,
      ) as string;
      const errorMessageField = this.getNodeParameter(
        "errorMessageField",
        itemIndex,
      ) as string;
      const attemptField = this.getNodeParameter(
        "attemptField",
        itemIndex,
      ) as string;
      const preserveOriginalItem = this.getNodeParameter(
        "preserveOriginalItem",
        itemIndex,
      ) as boolean;

      const statusCode = toNumber(getValueAtPath(item.json, statusCodeField));
      const errorMessage = toStringValue(
        getValueAtPath(item.json, errorMessageField),
      );
      const currentAttempt = Math.max(
        0,
        Math.floor(toNumber(getValueAtPath(item.json, attemptField)) ?? 0),
      );
      const nextAttempt = currentAttempt + 1;
      const hasFailureSignal =
        statusCode !== undefined
          ? statusCode >= 400
          : Boolean(errorMessage?.trim());
      const isRetryableStatus =
        statusCode !== undefined && retryStatusCodes.includes(statusCode);
      const isStopStatus =
        statusCode !== undefined && stopStatusCodes.includes(statusCode);
      const looksLikePermanentHttpFailure =
        statusCode !== undefined &&
        statusCode >= 400 &&
        !retryStatusCodes.includes(statusCode);

      let output = successItems;
      let shouldRetry = false;
      let exhausted = false;
      let reason = "No failure signal found";
      let delaySeconds = 0;

      if (hasFailureSignal) {
        if (isStopStatus || looksLikePermanentHttpFailure) {
          exhausted = true;
          reason =
            statusCode === undefined
              ? "Error is not retryable"
              : `Status code ${statusCode} is configured as permanent`;
          output = exhaustedItems;
        } else if (currentAttempt >= maxAttempts) {
          exhausted = true;
          reason = `Retry attempts exhausted at ${currentAttempt} of ${maxAttempts}`;
          output = exhaustedItems;
        } else if (isRetryableStatus || Boolean(errorMessage?.trim())) {
          shouldRetry = true;
          reason =
            statusCode === undefined
              ? "Error message found without a status code"
              : `Status code ${statusCode} is retryable`;
          delaySeconds = calculateDelaySeconds(
            strategy,
            nextAttempt,
            baseDelaySeconds,
            maxDelaySeconds,
            enableJitter,
            jitterPercentage,
          );
          output = retryItems;
        }
      }

      const metadata: IDataObject = {
        shouldRetry,
        attempt: shouldRetry ? nextAttempt : currentAttempt,
        maxAttempts,
        strategy,
        delaySeconds,
        reason,
        exhausted,
      };

      output.push(
        createOutputItem(
          item,
          itemIndex,
          preserveOriginalItem,
          "__retry",
          metadata,
        ),
      );
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unknown retry error";

      if (this.continueOnFail()) {
        exhaustedItems.push({
          json: { error: message },
          pairedItem: { item: itemIndex },
        });
        continue;
      }

      throw new NodeOperationError(this.getNode(), message, { itemIndex });
    }
  }

  return [successItems, retryItems, exhaustedItems];
}

async function executeConcurrencyGate(
  this: IExecuteFunctions,
): Promise<INodeExecutionData[][]> {
  const items = this.getInputData();
  const allowedItems: INodeExecutionData[] = [];
  const blockedItems: INodeExecutionData[] = [];
  const states = getConcurrencyStates(this);

  for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
    try {
      const item = items[itemIndex];
      const operation = this.getNodeParameter(
        "operation",
        itemIndex,
      ) as ConcurrencyOperation;
      const key = (
        this.getNodeParameter("gateKey", itemIndex) as string
      ).trim();
      const preserveOriginalItem = this.getNodeParameter(
        "preserveOriginalItem",
        itemIndex,
      ) as boolean;
      const now = Date.now();
      const state = getConcurrencyState(states, key);
      cleanExpiredLocks(state, now);

      let acquired = false;
      let routeBlocked = false;
      let lockId: string | undefined;
      let expiresAt: string | undefined;
      let reason = "Gate inspected";
      const maxConcurrent =
        operation === "release" || operation === "forceRelease"
          ? 1
          : Math.max(
              1,
              this.getNodeParameter("maxConcurrent", itemIndex) as number,
            );

      if (operation === "acquire") {
        const lockTtlSeconds = Math.max(
          1,
          this.getNodeParameter("lockTtlSeconds", itemIndex) as number,
        );
        const onLimitReached = this.getNodeParameter(
          "onLimitReached",
          itemIndex,
        ) as LimitMode;

        if (state.locks.length < maxConcurrent) {
          lockId = generateId("lock");
          expiresAt = isoFromTimestamp(now + lockTtlSeconds * 1000);

          const lock: ConcurrencyLock = {
            id: lockId,
            createdAt: isoFromTimestamp(now),
            expiresAt,
            executionId: this.getExecutionId(),
          };
          state.locks.push(lock);
          acquired = true;
          reason = "Lock acquired";
        } else {
          reason = "Concurrency limit reached";
          routeBlocked = onLimitReached === "routeBlocked";
        }
      }

      if (operation === "release") {
        const explicitLockId = (
          this.getNodeParameter("lockId", itemIndex) as string
        ).trim();
        const lockIdField = (
          this.getNodeParameter("lockIdField", itemIndex) as string
        ).trim();
        const fieldLockId = lockIdField
          ? toStringValue(getValueAtPath(item.json, lockIdField))
          : undefined;
        lockId = explicitLockId || fieldLockId;

        if (lockId) {
          const beforeCount = state.locks.length;
          state.locks = state.locks.filter((lock) => lock.id !== lockId);
          reason =
            state.locks.length < beforeCount
              ? "Lock released"
              : "Lock ID was not active";
        } else {
          reason = "No lock ID provided";
        }
      }

      if (operation === "forceRelease") {
        state.locks = [];
        reason = "All locks cleared for gate key";
      }

      const metadata: IDataObject = {
        key,
        acquired,
        activeCount: state.locks.length,
        maxConcurrent,
        reason,
        lockId,
        expiresAt,
        locks: operation === "inspect" ? state.locks : undefined,
      };

      const outputItem = createOutputItem(
        item,
        itemIndex,
        preserveOriginalItem,
        "__concurrencyGate",
        metadata,
      );

      if (routeBlocked) {
        blockedItems.push(outputItem);
      } else {
        allowedItems.push(outputItem);
      }
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unknown concurrency error";

      if (this.continueOnFail()) {
        blockedItems.push({
          json: { error: message },
          pairedItem: { item: itemIndex },
        });
        continue;
      }

      throw new NodeOperationError(this.getNode(), message, { itemIndex });
    }
  }

  return [allowedItems, blockedItems, []];
}

async function executeDeadLetterQueue(
  this: IExecuteFunctions,
): Promise<INodeExecutionData[][]> {
  const items = this.getInputData();
  const returnData: INodeExecutionData[] = [];
  const workflow = this.getWorkflow();

  for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
    try {
      const item = items[itemIndex];
      const operation = this.getNodeParameter("operation", itemIndex) as string;
      const failureReasonField = this.getNodeParameter(
        "failureReasonField",
        itemIndex,
      ) as string;
      const sourceNodeField = this.getNodeParameter(
        "sourceNodeField",
        itemIndex,
      ) as string;
      const workflowIdField = this.getNodeParameter(
        "workflowIdField",
        itemIndex,
      ) as string;
      const workflowNameField = this.getNodeParameter(
        "workflowNameField",
        itemIndex,
      ) as string;
      const executionIdField = this.getNodeParameter(
        "executionIdField",
        itemIndex,
      ) as string;
      const includeOriginalPayload = this.getNodeParameter(
        "includeOriginalPayload",
        itemIndex,
      ) as boolean;
      const includeErrorMetadata = this.getNodeParameter(
        "includeErrorMetadata",
        itemIndex,
      ) as boolean;
      const reason =
        toStringValue(getValueAtPath(item.json, failureReasonField)) ??
        toStringValue(getValueAtPath(item.json, "error.message")) ??
        toStringValue(getValueAtPath(item.json, "error")) ??
        "Failed item";
      const error = includeErrorMetadata
        ? removeUndefinedValues({
            error: getValueAtPath(item.json, "error"),
            message: getValueAtPath(item.json, "message"),
            statusCode: getValueAtPath(item.json, "statusCode"),
            concurrencyGate: getValueAtPath(item.json, "__concurrencyGate"),
          })
        : {};
      const deadLetter: IDataObject = removeUndefinedValues({
        deadLetterId: generateId("dlq"),
        timestamp: new Date().toISOString(),
        operation,
        reason,
        sourceNode:
          toStringValue(getValueAtPath(item.json, sourceNodeField)) ??
          this.getNode().name,
        workflowId:
          toStringValue(getValueAtPath(item.json, workflowIdField)) ??
          workflow.id,
        workflowName:
          toStringValue(getValueAtPath(item.json, workflowNameField)) ??
          workflow.name,
        executionId:
          toStringValue(getValueAtPath(item.json, executionIdField)) ??
          this.getExecutionId(),
        originalPayload: includeOriginalPayload
          ? deepCopy(item.json)
          : undefined,
        error,
        retry: includeErrorMetadata
          ? asDataObject(getValueAtPath(item.json, "__retry"))
          : {},
      });

      returnData.push({
        json: deadLetter,
        pairedItem: { item: itemIndex },
      });
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unknown dead-letter error";

      if (this.continueOnFail()) {
        returnData.push({
          json: { error: message },
          pairedItem: { item: itemIndex },
        });
        continue;
      }

      throw new NodeOperationError(this.getNode(), message, { itemIndex });
    }
  }

  return [returnData, [], []];
}
