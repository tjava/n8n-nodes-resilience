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
  asDataObject,
  deepCopy,
  generateId,
  removeUndefinedValues,
  toStringValue,
} from "../shared/utils";

type DeadLetterOperation = "formatFailedItem";

export class DeadLetterQueue implements INodeType {
  description: INodeTypeDescription = {
    displayName: "Dead Letter Queue",
    name: "deadLetterQueue",
    icon: "file:deadLetterQueue.svg",
    group: ["transform"],
    version: 1,
    subtitle: '={{$parameter["operation"]}}',
    description: "Format failed workflow items for review or replay",
    defaults: {
      name: "Dead Letter Queue",
    },
    inputs: [NodeConnectionTypes.Main],
    outputs: [
      {
        type: NodeConnectionTypes.Main,
        displayName: "Dead Letter Item",
      },
    ],
    usableAsTool: true,
    properties: [
      {
        displayName: "Operation",
        name: "operation",
        type: "options",
        noDataExpression: true,
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
        displayName: "Failure Reason Field",
        name: "failureReasonField",
        type: "string",
        default: "message",
        description:
          "Dot path containing the failure reason. Falls back to error metadata when empty.",
      },
      {
        displayName: "Source Node Field",
        name: "sourceNodeField",
        type: "string",
        default: "sourceNode",
        description: "Optional dot path containing the source node name",
      },
      {
        displayName: "Workflow ID Field",
        name: "workflowIdField",
        type: "string",
        default: "workflowId",
        description: "Optional dot path containing a workflow ID override",
      },
      {
        displayName: "Workflow Name Field",
        name: "workflowNameField",
        type: "string",
        default: "workflowName",
        description: "Optional dot path containing a workflow name override",
      },
      {
        displayName: "Execution ID Field",
        name: "executionIdField",
        type: "string",
        default: "executionId",
        description: "Optional dot path containing an execution ID override",
      },
      {
        displayName: "Include Original Payload",
        name: "includeOriginalPayload",
        type: "boolean",
        default: true,
        description: "Whether to include the incoming JSON as originalPayload",
      },
      {
        displayName: "Include Error Metadata",
        name: "includeErrorMetadata",
        type: "boolean",
        default: true,
        description:
          "Whether to include error, retry, circuit breaker, and gate metadata",
      },
      {
        displayName: "Destination Mode",
        name: "destinationMode",
        type: "options",
        noDataExpression: true,
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
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData();
    const returnData: INodeExecutionData[] = [];
    const workflow = this.getWorkflow();

    for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
      try {
        const item = items[itemIndex];
        const operation = this.getNodeParameter(
          "operation",
          itemIndex,
        ) as DeadLetterOperation;
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

    return [returnData];
  }
}
