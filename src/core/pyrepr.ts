/**
 * Python `repr()` and type-name rendering.
 *
 * pydantic validation messages embed both the offending value's `repr()` and
 * its `type(...).__name__`, so reproducing those messages means reproducing
 * these two functions for the JSON value space.
 */

import { isPyFloat } from "./pyfloat.js";

/** `type(value).__name__` for a JSON value. */
export function pythonTypeName(value: unknown): string {
  if (value === null || value === undefined) return "NoneType";
  if (Array.isArray(value)) return "list";
  if (isPyFloat(value)) return "float";
  switch (typeof value) {
    case "string":
      return "str";
    case "boolean":
      return "bool";
    case "number":
      return Number.isInteger(value) ? "int" : "float";
    case "object":
      return "dict";
    default:
      return "object";
  }
}

/** Quote a string the way Python's `repr()` does. */
function reprString(value: string): string {
  const escaped = value
    .replace(/\\/g, "\\\\")
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r")
    .replace(/\t/g, "\\t");
  // Python prefers single quotes unless the string contains one and no double.
  if (escaped.includes("'") && !escaped.includes('"')) {
    return `"${escaped}"`;
  }
  return `'${escaped.replace(/'/g, "\\'")}'`;
}

/** `repr(value)` for a JSON value. */
export function pythonRepr(value: unknown): string {
  if (value === null || value === undefined) return "None";
  if (isPyFloat(value)) {
    const primitive = value.valueOf();
    return Number.isInteger(primitive) ? `${primitive}.0` : String(primitive);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => pythonRepr(item)).join(", ")}]`;
  }
  switch (typeof value) {
    case "string":
      return reprString(value);
    case "boolean":
      return value ? "True" : "False";
    case "number":
      if (Number.isInteger(value)) return String(value);
      return String(value);
    case "object": {
      const entries = Object.entries(value as Record<string, unknown>).map(
        ([key, item]) => `${reprString(key)}: ${pythonRepr(item)}`,
      );
      return `{${entries.join(", ")}}`;
    }
    default:
      return String(value);
  }
}
