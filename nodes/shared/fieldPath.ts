import { IDataObject } from "n8n-workflow";

export function splitFieldPath(path: string): string[] {
  return path
    .split(".")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

export function getValueAtPath(
  data: IDataObject,
  path: string,
): IDataObject[keyof IDataObject] | undefined {
  const parts = splitFieldPath(path);
  let current: unknown = data;

  for (const part of parts) {
    if (!isPlainObject(current) && !Array.isArray(current)) {
      return undefined;
    }

    current = (current as Record<string, unknown>)[part];
  }

  return current as IDataObject[keyof IDataObject] | undefined;
}

export function setValueAtPath(
  data: IDataObject,
  path: string,
  value: unknown,
): IDataObject {
  const parts = splitFieldPath(path);

  if (parts.length === 0) {
    return data;
  }

  let current: Record<string, unknown> = data;

  for (let index = 0; index < parts.length - 1; index++) {
    const part = parts[index];
    const existing = current[part];

    if (!isPlainObject(existing)) {
      current[part] = {};
    }

    current = current[part] as Record<string, unknown>;
  }

  current[parts[parts.length - 1]] = value;

  return data;
}

export function isPlainObject(value: unknown): value is IDataObject {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
