/**
 * Error helpers.
 *
 * The MCP tool layer surfaces failures as `Error executing tool <name>: <text>`,
 * where `<text>` is Python's `str(exception)`. `str()` on an exception yields
 * only its message (no class-name prefix), so this helper does the same for
 * JavaScript errors.
 */

export function errorText(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

/** Raised for invalid tool arguments; mirrors Python's `ValueError`. */
export class ValueError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ValueError";
  }
}
