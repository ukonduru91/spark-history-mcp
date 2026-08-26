/**
 * Argument validation compatible with pydantic v2.
 *
 * FastMCP validates tool arguments against a pydantic model built from the
 * Python function signature, so a client that passes a wrong-typed argument
 * gets a very specific error string back. Clients (and the models driving them)
 * read those messages, so the port reproduces both the coercion rules and the
 * message format.
 *
 * Coercion rules reproduced here, verified against pydantic 2.13 running in the
 * upstream server:
 *
 * | Field type | Accepts | Rejects with |
 * | --- | --- | --- |
 * | `string` | strings only | `string_type` |
 * | `integer` | ints, booleans, integral floats, numeric strings | `int_from_float`, `int_parsing`, `int_type` |
 * | `boolean` | booleans, `0`/`1`, `"true"`/`"yes"`/`"on"`/… | `bool_parsing`, `bool_type` |
 * | `array` | lists (items validated per element) | `list_type` |
 * | `anyOf [T, null]` | `null`, or whatever `T` accepts | `T`'s error |
 *
 * Unknown arguments are ignored, matching the Python model's default.
 */

import { pythonRepr, pythonTypeName } from "./pyrepr.js";

/** The pydantic version whose error-documentation URLs are reproduced. */
const PYDANTIC_VERSION = "2.13";

interface FieldError {
  location: string;
  message: string;
  type: string;
  inputValue: unknown;
}

export class ValidationError extends Error {
  constructor(title: string, errors: FieldError[]) {
    super(ValidationError.format(title, errors));
    this.name = "ValidationError";
  }

  private static format(title: string, errors: FieldError[]): string {
    const count = errors.length;
    const heading = `${count} validation error${count === 1 ? "" : "s"} for ${title}`;
    const blocks = errors.map(
      (error) =>
        `${error.location}\n  ${error.message} [type=${error.type}, ` +
        `input_value=${pythonRepr(error.inputValue)}, ` +
        `input_type=${pythonTypeName(error.inputValue)}]\n` +
        `    For further information visit https://errors.pydantic.dev/${PYDANTIC_VERSION}/v/${error.type}`,
    );
    return [heading, ...blocks].join("\n");
  }
}

type JsonSchema = Record<string, any>;

const TRUE_STRINGS = new Set(["1", "true", "t", "yes", "y", "on"]);
const FALSE_STRINGS = new Set(["0", "false", "f", "no", "n", "off"]);

/** Unwrap `anyOf: [T, {type: "null"}]` into T, as pydantic's `Optional[T]`. */
function unwrapOptional(schema: JsonSchema): { inner: JsonSchema; optional: boolean } {
  if (Array.isArray(schema.anyOf)) {
    const branches = schema.anyOf as JsonSchema[];
    const nonNull = branches.filter((branch) => branch.type !== "null");
    if (nonNull.length === 1 && nonNull.length !== branches.length) {
      return { inner: nonNull[0], optional: true };
    }
    if (nonNull.length >= 1) {
      return { inner: nonNull[0], optional: branches.length !== nonNull.length };
    }
  }
  return { inner: schema, optional: false };
}

/**
 * Validate and coerce one value against a leaf schema.
 *
 * Returns the coerced value, or pushes an error and returns the input.
 */
function coerce(
  value: unknown,
  schema: JsonSchema,
  location: string,
  errors: FieldError[],
): unknown {
  switch (schema.type) {
    case "string": {
      if (typeof value === "string") return value;
      errors.push({
        location,
        message: "Input should be a valid string",
        type: "string_type",
        inputValue: value,
      });
      return value;
    }
    case "integer": {
      if (typeof value === "boolean") return value ? 1 : 0;
      if (typeof value === "number") {
        if (Number.isInteger(value)) return value;
        errors.push({
          location,
          message:
            "Input should be a valid integer, got a number with a fractional part",
          type: "int_from_float",
          inputValue: value,
        });
        return value;
      }
      if (typeof value === "string") {
        const trimmed = value.trim();
        // pydantic accepts an integral decimal string; anything else fails.
        if (/^[+-]?\d+$/.test(trimmed)) return Number(trimmed);
        const asNumber = Number(trimmed);
        if (trimmed !== "" && Number.isFinite(asNumber) && Number.isInteger(asNumber)) {
          return asNumber;
        }
        errors.push({
          location,
          message: "Input should be a valid integer, unable to parse string as an integer",
          type: "int_parsing",
          inputValue: value,
        });
        return value;
      }
      errors.push({
        location,
        message: "Input should be a valid integer",
        type: "int_type",
        inputValue: value,
      });
      return value;
    }
    case "boolean": {
      if (typeof value === "boolean") return value;
      if (typeof value === "number") {
        if (value === 0) return false;
        if (value === 1) return true;
        errors.push({
          location,
          message: "Input should be a valid boolean, unable to interpret input",
          type: "bool_parsing",
          inputValue: value,
        });
        return value;
      }
      if (typeof value === "string") {
        const lowered = value.trim().toLowerCase();
        if (TRUE_STRINGS.has(lowered)) return true;
        if (FALSE_STRINGS.has(lowered)) return false;
        errors.push({
          location,
          message: "Input should be a valid boolean, unable to interpret input",
          type: "bool_parsing",
          inputValue: value,
        });
        return value;
      }
      errors.push({
        location,
        message: "Input should be a valid boolean",
        type: "bool_type",
        inputValue: value,
      });
      return value;
    }
    case "array": {
      if (!Array.isArray(value)) {
        errors.push({
          location,
          message: "Input should be a valid list",
          type: "list_type",
          inputValue: value,
        });
        return value;
      }
      const itemSchema = (schema.items as JsonSchema) ?? {};
      return value.map((item, index) =>
        coerce(item, itemSchema, `${location}.${index}`, errors),
      );
    }
    default:
      return value;
  }
}

/**
 * Validate a tool call's arguments against its JSON Schema.
 *
 * @returns the coerced arguments
 * @throws ValidationError formatted like pydantic's
 */
export function validateToolArguments(
  toolName: string,
  schema: JsonSchema,
  args: Record<string, unknown>,
): Record<string, unknown> {
  const properties = (schema.properties as Record<string, JsonSchema>) ?? {};
  const required = new Set((schema.required as string[]) ?? []);
  const errors: FieldError[] = [];
  const out: Record<string, unknown> = { ...args };

  // Iterate in schema order, which is the Python function's signature order —
  // pydantic reports errors in that order too.
  for (const [name, propertySchema] of Object.entries(properties)) {
    const present = Object.prototype.hasOwnProperty.call(args, name);

    if (!present) {
      if (required.has(name)) {
        errors.push({
          location: name,
          message: "Field required",
          type: "missing",
          inputValue: args,
        });
      } else if (Object.prototype.hasOwnProperty.call(propertySchema, "default")) {
        out[name] = propertySchema.default;
      }
      continue;
    }

    const { inner, optional } = unwrapOptional(propertySchema);
    const value = args[name];
    if (value === null || value === undefined) {
      if (optional) {
        out[name] = null;
        continue;
      }
      // A required, non-optional field rejects null with its own type error.
      out[name] = coerce(value, inner, name, errors);
      continue;
    }
    out[name] = coerce(value, inner, name, errors);
  }

  if (errors.length > 0) {
    throw new ValidationError(`${toolName}Arguments`, errors);
  }

  return out;
}

/**
 * Validate a prompt's arguments.
 *
 * FastMCP checks for missing arguments before rendering and lets pydantic
 * reject unexpected keywords when it calls the prompt function, so the two
 * failures have different message shapes.
 */
export function validatePromptArguments(
  promptName: string,
  known: Array<{ name: string; required: boolean }>,
  args: Record<string, unknown>,
): void {
  const missing = known
    .filter((argument) => argument.required && args[argument.name] === undefined)
    .map((argument) => argument.name);
  if (missing.length > 0) {
    // Python renders the set of names, e.g. `{'app_id'}`.
    const rendered = missing.map((name) => `'${name}'`).join(", ");
    throw new Error(`Missing required arguments: {${rendered}}`);
  }

  const knownNames = new Set(known.map((argument) => argument.name));
  for (const [name, value] of Object.entries(args)) {
    if (!knownNames.has(name)) {
      throw new ValidationError(promptName, [
        {
          location: name,
          message: "Unexpected keyword argument",
          type: "unexpected_keyword_argument",
          inputValue: value,
        },
      ]);
    }
  }
}
