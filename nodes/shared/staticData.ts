import { IDataObject, IExecuteFunctions } from "n8n-workflow";

import { isPlainObject } from "./fieldPath";

export interface ConcurrencyLock extends IDataObject {
  id: string;
  createdAt: string;
  expiresAt: string;
  executionId?: string;
}

export interface ConcurrencyState extends IDataObject {
  locks: ConcurrencyLock[];
}

const CONCURRENCY_NAMESPACE = "resilienceConcurrencyGate";

export function getConcurrencyStates(
  node: IExecuteFunctions,
): Record<string, ConcurrencyState> {
  const staticData = node.getWorkflowStaticData("global");

  if (!isPlainObject(staticData[CONCURRENCY_NAMESPACE])) {
    staticData[CONCURRENCY_NAMESPACE] = {};
  }

  const states = staticData[CONCURRENCY_NAMESPACE] as Record<
    string,
    ConcurrencyState
  >;

  return states;
}

export function getConcurrencyState(
  states: Record<string, ConcurrencyState>,
  key: string,
): ConcurrencyState {
  const state = states[key];

  if (!state) {
    const newState: ConcurrencyState = { locks: [] };
    states[key] = newState;
    return newState;
  }

  if (!Array.isArray(state.locks)) {
    state.locks = [];
  }

  return state;
}

export function cleanExpiredLocks(state: ConcurrencyState, now: number): void {
  state.locks = state.locks.filter((lock) => Date.parse(lock.expiresAt) > now);
}
