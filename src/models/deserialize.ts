/**
 * Runtime deserialization of Spark History Server REST payloads into the
 * generated model shapes.
 *
 * This mirrors what the Python project's OpenAPI-generated client does:
 *
 * * `Model.from_dict()` picks the known properties out of the raw JSON by their
 *   alias, recursing into nested models, and stores everything it does not know
 *   about under `additional_properties`;
 * * FastMCP then serializes the model with `model_dump(mode="json",
 *   by_alias=True)`, which emits every known field — `null` when absent — in
 *   field-definition order, followed by `additional_properties`.
 *
 * Reproducing both halves here keeps the ported server's JSON responses
 * byte-identical to the Python server's.
 */

import { isPyFloat, pyFloat } from "../core/pyfloat.js";
import { MODEL_SCHEMAS, type FieldDescriptor } from "./generated.js";

function convert(value: unknown, desc: FieldDescriptor): unknown {
  if (value === undefined || value === null) {
    return null;
  }
  switch (desc.k) {
    case "m":
      return fromDict(desc.m as string, value);
    case "l":
      return Array.isArray(value)
        ? value.map((item) => convert(item, desc.i as FieldDescriptor))
        : null;
    case "d": {
      if (typeof value !== "object" || Array.isArray(value)) return null;
      const out: Record<string, unknown> = {};
      for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
        out[key] = convert(item, desc.i as FieldDescriptor);
      }
      return out;
    }
    default:
      return value;
  }
}

/**
 * Build a model instance from a raw JSON object.
 *
 * Returns `null` for `null`/`undefined` input, matching `from_dict`.
 */
export function fromDict<T = any>(modelName: string, raw: unknown): T | null {
  if (raw === undefined || raw === null) return null;
  const fields = MODEL_SCHEMAS[modelName];
  if (!fields) {
    throw new Error(`Unknown model: ${modelName}`);
  }
  if (typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error(`Expected an object for model ${modelName}`);
  }

  const source = raw as Record<string, unknown>;
  const known = new Set(fields.map((field) => field.alias));
  const out: Record<string, unknown> = {};

  for (const field of fields) {
    out[field.alias] = convert(source[field.alias], field.desc);
  }

  const extra: Record<string, unknown> = {};
  for (const key of Object.keys(source)) {
    if (!known.has(key)) {
      extra[key] = source[key];
    }
  }
  out.additional_properties = extra;

  return out as T;
}

/** Deserialize a JSON array into a list of models. */
export function fromDictList<T = any>(modelName: string, raw: unknown): T[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((item) => fromDict<T>(modelName, item) as T);
}

/**
 * Deep copy of a model, equivalent to pydantic's `model_copy(deep=True)`.
 *
 * Hand-rolled rather than `structuredClone` so that {@link PyFloat} boxes keep
 * their class (and therefore their float-ness) across the copy.
 */
export function modelCopyDeep<T>(value: T): T {
  if (value === null || value === undefined) return value;
  if (isPyFloat(value)) return pyFloat(value.valueOf()) as unknown as T;
  if (Array.isArray(value)) {
    return value.map((item) => modelCopyDeep(item)) as unknown as T;
  }
  if (typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
      out[key] = modelCopyDeep(item);
    }
    return out as unknown as T;
  }
  return value;
}
