/**
 * Float-vs-int fidelity.
 *
 * Python distinguishes `114` from `114.0`, and `json.dumps` renders them
 * differently. JavaScript has one number type, so a Spark payload containing
 * `114.0` would otherwise be re-serialized as `114`, making the ported server's
 * text output differ from the Python server's.
 *
 * `PyFloat` is a `Number` subclass used to carry "this value was a float". It
 * behaves like a number in every arithmetic and comparison context (via
 * `valueOf`), and {@link ../core/json.js} renders it the way Python's `repr`
 * would.
 *
 * Two sources create them:
 *
 * * {@link parseJsonPreservingFloats} — a JSON literal written with a decimal
 *   point or exponent (this is how Spark reports task-metric distributions);
 * * {@link pyFloat} — a value the Python code computes with `/`, which always
 *   produces a float.
 */

export class PyFloat extends Number {}

/** Mark a computed value as a Python float. */
export function pyFloat(value: number): PyFloat {
  return new PyFloat(value);
}

export function isPyFloat(value: unknown): value is PyFloat {
  return value instanceof PyFloat;
}

/**
 * `JSON.parse` that boxes float literals as {@link PyFloat}.
 *
 * Uses the reviver's source-text argument (Node >= 21) to see how the number was
 * written; on older runtimes it degrades to plain numbers, which only affects
 * `114.0` vs `114` rendering.
 */
export function parseJsonPreservingFloats(text: string): unknown {
  return JSON.parse(text, function (_key, value, context?: { source?: string }) {
    if (typeof value === "number" && context && typeof context.source === "string") {
      if (/[.eE]/.test(context.source)) {
        return new PyFloat(value);
      }
    }
    return value;
  });
}

/** Render a float the way Python's `repr()` does (`114` -> `114.0`). */
export function formatPythonFloat(value: number): string {
  if (!Number.isFinite(value)) {
    return value > 0 ? "Infinity" : Number.isNaN(value) ? "NaN" : "-Infinity";
  }
  const text = String(value);
  return /[.eE]/.test(text) ? text : `${text}.0`;
}

/**
 * `sum()` over a list of numbers.
 *
 * Python's `sum` returns an int when every operand is an int, and a float as
 * soon as one operand is a float — which decides whether the result renders as
 * `0` or `0.0`.
 */
export function pySum(values: Array<number | PyFloat>): number | PyFloat {
  let total = 0;
  let float = false;
  for (const value of values) {
    if (isPyFloat(value)) float = true;
    total += Number(value);
  }
  return float ? pyFloat(total) : total;
}

/** Python's `/`, which always produces a float. */
export function pyDiv(left: number | PyFloat, right: number | PyFloat): PyFloat {
  return pyFloat(Number(left) / Number(right));
}
