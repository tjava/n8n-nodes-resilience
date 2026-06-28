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
import {
  cleanExpiredLocks,
  ConcurrencyLock,
  getConcurrencyState,
  getConcurrencyStates,
} from "../shared/staticData";
import {
  createOutputItem,
  generateId,
  isoFromTimestamp,
  toStringValue,
} from "../shared/utils";

type ConcurrencyOperation = "acquire" | "release" | "inspect" | "forceRelease";
type LimitMode = "routeBlocked" | "allowWithMetadata";

export class ConcurrencyGate implements INodeType {
  description: INodeTypeDescription = {
    displayName: "Concurrency Gate",
    name: "concurrencyGate",
    icon: {
      light: "file:concurrencyGate.svg",
      dark: "file:concurrencyGate.dark.svg",
    },
    group: ["transform"],
    version: 1,
    subtitle: '={{$parameter["operation"]}}',
    description:
      "Best-effort static-data concurrency gate for workflow sections",
    defaults: {
      name: "Concurrency Gate",
    },
    inputs: [NodeConnectionTypes.Main],
    outputs: [
      {
        type: NodeConnectionTypes.Main,
        displayName: "Acquired / Allowed",
      },
      {
        type: NodeConnectionTypes.Main,
        displayName: "Blocked",
      },
    ],
    usableAsTool: true,
    outputNames: ["Acquired / Allowed", "Blocked"],
    properties: [
      {
        displayName: "Operation",
        name: "operation",
        type: "options",
        noDataExpression: true,
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
        displayName: "Gate Key",
        name: "gateKey",
        type: "string",
        required: true,
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
            operation: ["release"],
          },
        },
        default: "",
        description:
          "Optional explicit lock ID to release. Takes precedence over Lock ID Field.",
      },
      {
        displayName: "Preserve Original Item",
        name: "preserveOriginalItem",
        type: "boolean",
        default: true,
        description:
          "Whether to keep the incoming JSON and binary data when adding gate metadata",
      },
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
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

    return [allowedItems, blockedItems];
  }
}
