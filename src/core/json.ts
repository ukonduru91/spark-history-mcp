/**
 * JSON serialization matching Python's `json.dumps(value, indent=2)`.
 *
 * FastMCP turns each tool result into text content with
 * `json.dumps(obj, indent=2)`. Two of its defaults differ from
 * `JSON.stringify`:
 *
 * * `ensure_ascii=True` escapes every non-ASCII character as `\uXXXX`;
 * * floats render through `repr()`, so `114.0` stays `114.0`.
 *
 * Both are reproduced here; see {@link ./pyfloat.js} for how float-ness is
 * carried through the pipeline.
 */

import { formatPythonFloat, isPyFloat } from "./pyfloat.js";

const NON_ASCII = /[^\x20-\x7E]/g;

/** Escape non-ASCII characters in an already-JSON-quoted string. */
function escapeNonAscii(text: string): string {
  return text.replace(NON_ASCII, (char) => {
    const code = char.codePointAt(0) as number;
    if (code <= 0xffff) {
      return `\\u${code.toString(16).padStart(4, "0")}`;
    }
    const high = Math.floor((code - 0x10000) / 0x400) + 0xd800;
    const low = ((code - 0x10000) % 0x400) + 0xdc00;
    return `\\u${high.toString(16).padStart(4, "0")}\\u${low
      .toString(16)
      .padStart(4, "0")}`;
  });
}

function quote(text: string): string {
  return escapeNonAscii(JSON.stringify(text));
}

function writeValue(value: unknown, indent: number, level: number): string {
  if (value === null || value === undefined) return "null";

  if (isPyFloat(value)) {
    return formatPythonFloat(value.valueOf());
  }

  const primitive =
    value instanceof Number
      ? value.valueOf()
      : value instanceof String
        ? value.valueOf()
        : value instanceof Boolean
          ? value.valueOf()
          : value;

  switch (typeof primitive) {
    case "string":
      return quote(primitive);
    case "boolean":
      return primitive ? "true" : "false";
    case "number":
      if (!Number.isFinite(primitive)) {
        return Number.isNaN(primitive)
          ? "NaN"
          : primitive > 0
            ? "Infinity"
            : "-Infinity";
      }
      return Number.isInteger(primitive)
        ? String(primitive)
        : formatPythonFloat(primitive);
    default:
      break;
  }

  const pad = " ".repeat(indent * (level + 1));
  const closePad = " ".repeat(indent * level);

  if (Array.isArray(primitive)) {
    if (primitive.length === 0) return "[]";
    const items = primitive.map(
      (item) => `${pad}${writeValue(item, indent, level + 1)}`,
    );
    return `[\n${items.join(",\n")}\n${closePad}]`;
  }

  if (typeof primitive === "object") {
    const entries = Object.entries(primitive as Record<string, unknown>).filter(
      ([, item]) => item !== undefined,
    );
    if (entries.length === 0) return "{}";
    const rendered = entries.map(
      ([key, item]) =>
        `${pad}${quote(key)}: ${writeValue(item, indent, level + 1)}`,
    );
    return `{\n${rendered.join(",\n")}\n${closePad}}`;
  }

  return "null";
}

/** `json.dumps(value, indent=2)` with `ensure_ascii=True`. */
export function pythonJsonDumps(value: unknown): string {
  return writeValue(value, 2, 0);
}
