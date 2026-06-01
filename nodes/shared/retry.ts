export type RetryStrategyName = "fixed" | "linear" | "exponential";

export function calculateDelaySeconds(
  strategy: RetryStrategyName,
  attempt: number,
  baseDelaySeconds: number,
  maxDelaySeconds: number,
  enableJitter: boolean,
  jitterPercentage: number,
): number {
  const safeAttempt = Math.max(1, attempt);
  const safeBaseDelay = Math.max(0, baseDelaySeconds);
  const safeMaxDelay = Math.max(0, maxDelaySeconds);
  let delay = safeBaseDelay;

  if (strategy === "linear") {
    delay = safeBaseDelay * safeAttempt;
  }

  if (strategy === "exponential") {
    delay = safeBaseDelay * Math.pow(2, safeAttempt - 1);
  }

  delay = Math.min(delay, safeMaxDelay);

  if (enableJitter && jitterPercentage > 0 && delay > 0) {
    const jitterRange = delay * (Math.max(0, jitterPercentage) / 100);
    const jitterOffset = Math.random() * jitterRange * 2 - jitterRange;
    delay += jitterOffset;
  }

  return Math.max(0, Math.round(delay));
}
