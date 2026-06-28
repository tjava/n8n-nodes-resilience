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
  createOutputItem,
  parseNumberList,
  toNumber,
  toStringValue,
} from "../shared/utils";

export class RetryStrategy implements INodeType {
  description: INodeTypeDescription = {
    displayName: "Retry Strategy",
    name: "retryStrategy",
    icon: {
      light: "file:retryStrategy.svg",
      dark: "file:retryStrategy.dark.svg",
    },
    group: ["transform"],
    version: 1,
    subtitle: '={{$parameter["strategy"]}}',
    description: "Route items by retry policy without sleeping inside the node",
    defaults: {
      name: "Retry Strategy",
    },
    inputs: [NodeConnectionTypes.Main],
    outputs: [
      {
        type: NodeConnectionTypes.Main,
        displayName: "Success",
      },
      {
        type: NodeConnectionTypes.Main,
        displayName: "Retry",
      },
      {
        type: NodeConnectionTypes.Main,
        displayName: "Exhausted",
      },
    ],
    usableAsTool: true,
    outputNames: ["Success", "Retry", "Exhausted"],
    properties: [
      {
        displayName: "Max Attempts",
        name: "maxAttempts",
        type: "number",
        typeOptions: {
          minValue: 1,
        },
        default: 3,
        description: "Maximum retry attempts before the item is exhausted",
      },
      {
        displayName: "Strategy",
        name: "strategy",
        type: "options",
        noDataExpression: true,
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
        default: 300,
        description:
          "Maximum delay to output after strategy and jitter are applied",
      },
      {
        displayName: "Enable Jitter",
        name: "enableJitter",
        type: "boolean",
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
        default: "408,429,500,502,503,504",
        description: "Comma-separated HTTP status codes that should be retried",
      },
      {
        displayName: "Stop Status Codes",
        name: "stopStatusCodes",
        type: "string",
        default: "400,401,403,404",
        description:
          "Comma-separated HTTP status codes that should fail permanently",
      },
      {
        displayName: "Status Code Field",
        name: "statusCodeField",
        type: "string",
        default: "statusCode",
        description: "Dot path containing the status code on each input item",
      },
      {
        displayName: "Error Message Field",
        name: "errorMessageField",
        type: "string",
        default: "message",
        description: "Dot path containing the error message on each input item",
      },
      {
        displayName: "Attempt Field",
        name: "attemptField",
        type: "string",
        default: "__retry.attempt",
        description:
          "Dot path containing the last retry attempt count on each input item",
      },
      {
        displayName: "Preserve Original Item",
        name: "preserveOriginalItem",
        type: "boolean",
        default: true,
        description:
          "Whether to keep the incoming JSON and binary data when adding retry metadata",
      },
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
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
}
