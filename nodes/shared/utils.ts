import { IDataObject, INodeExecutionData } from "n8n-workflow";

import { isPlainObject, setValueAtPath } from "./fieldPath";

export function deepCopy<T>(value: T): T {
  if (value === undefined || value === null) {
    return value;
  }

  return JSON.parse(JSON.stringify(value)) as T;
}

export function toNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }

  return undefined;
}

export function toStringValue(value: unknown): string | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }

  if (typeof value === "string") {
    return value;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  return undefined;
}

export function parseNumberList(value: string): number[] {
  return value
    .split(",")
    .map((entry) => Number(entry.trim()))
    .filter((entry) => Number.isInteger(entry));
}

export function createOutputItem(
  item: INodeExecutionData,
  itemIndex: number,
  preserveOriginalItem: boolean,
  metadataPath: string,
  metadata: IDataObject,
): INodeExecutionData {
  const json = preserveOriginalItem ? deepCopy(item.json) : {};
  setValueAtPath(json, metadataPath, metadata);

  return {
    json,
    binary: preserveOriginalItem ? item.binary : undefined,
    pairedItem: { item: itemIndex },
  };
}

export function removeUndefinedValues(data: IDataObject): IDataObject {
  return Object.fromEntries(
    Object.entries(data).filter(([, value]) => value !== undefined),
  ) as IDataObject;
}

export function asDataObject(value: unknown): IDataObject {
  return isPlainObject(value) ? value : {};
}

export function generateId(prefix: string): string {
  const random = Math.random().toString(36).slice(2, 10);
  return `${prefix}_${Date.now().toString(36)}_${random}`;
}

export function isoFromTimestamp(timestamp: number): string {
  return new Date(timestamp).toISOString();
}
