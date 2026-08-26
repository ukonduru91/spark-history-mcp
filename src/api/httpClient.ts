/**
 * Minimal HTTP client for the Spark History Server REST API.
 *
 * Stands in for the `urllib3`-based transport of the Python project's
 * OpenAPI-generated client, keeping the same error taxonomy (`ApiException` and
 * its status-specific subclasses) and the same message formatting, because the
 * MCP tools surface `str(exception)` to the caller.
 */

import http from "node:http";
import https from "node:https";
import { URL } from "node:url";

import { parseJsonPreservingFloats } from "../core/pyfloat.js";

export class OpenApiException extends Error {}

export class ApiException extends OpenApiException {
  readonly status: number | null;
  readonly reason: string | null;
  readonly headers: Record<string, string>;
  readonly body: string | null;
  readonly data: unknown;

  constructor(
    status: number | null,
    reason: string | null,
    headers: Record<string, string> = {},
    body: string | null = null,
    data: unknown = null,
  ) {
    super("");
    this.name = new.target.name;
    this.status = status;
    this.reason = reason;
    this.headers = headers;
    this.body = body;
    this.data = data;
    this.message = this.format();
  }

  /** Mirrors `ApiException.__str__` in the generated Python client. */
  private format(): string {
    let message = `(${this.status})\nReason: ${this.reason}\n`;
    if (this.headers && Object.keys(this.headers).length > 0) {
      message += `HTTP response headers: ${formatHeaders(this.headers)}\n`;
    }
    if (this.body) {
      message += `HTTP response body: ${this.body}\n`;
    }
    if (this.data) {
      message += `HTTP response data: ${String(this.data)}\n`;
    }
    return message;
  }

  toString(): string {
    return this.message;
  }
}

export class BadRequestException extends ApiException {}
export class UnauthorizedException extends ApiException {}
export class ForbiddenException extends ApiException {}
export class NotFoundException extends ApiException {}
export class ConflictException extends ApiException {}
export class UnprocessableEntityException extends ApiException {}
export class ServiceException extends ApiException {}

/** Render headers the way Python's `urllib3.HTTPHeaderDict` repr does. */
function formatHeaders(headers: Record<string, string>): string {
  const entries = Object.entries(headers)
    .map(([key, value]) => `'${key}': '${String(value).replace(/'/g, "\\'")}'`)
    .join(", ");
  return `HTTPHeaderDict({${entries}})`;
}

/** Raise the exception class matching an HTTP status, as `from_response` does. */
export function throwForStatus(
  status: number,
  reason: string,
  headers: Record<string, string>,
  body: string | null,
): never {
  const args: [number, string, Record<string, string>, string | null] = [
    status,
    reason,
    headers,
    body,
  ];
  if (status === 400) throw new BadRequestException(...args);
  if (status === 401) throw new UnauthorizedException(...args);
  if (status === 403) throw new ForbiddenException(...args);
  if (status === 404) throw new NotFoundException(...args);
  if (status === 409) throw new ConflictException(...args);
  if (status === 422) throw new UnprocessableEntityException(...args);
  if (status >= 500 && status <= 599) throw new ServiceException(...args);
  throw new ApiException(...args);
}

export type QueryValue = string | number | boolean | null | undefined;

export interface RestClientOptions {
  verifySsl?: boolean;
  sslCaCert?: string | null;
  /** SOCKS5 proxy URL, e.g. `socks5h://localhost:8157`. */
  proxy?: string | null;
  /** Request timeout in seconds. */
  timeout?: number;
}

export class RestClient {
  private readonly baseUrl: string;
  private readonly options: RestClientOptions;
  private readonly defaultHeaders: Record<string, string> = {};
  private agent: http.Agent | https.Agent | undefined;

  constructor(baseUrl: string, options: RestClientOptions = {}) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.options = options;
  }

  setDefaultHeader(name: string, value: string): void {
    this.defaultHeaders[name] = value;
  }

  removeDefaultHeader(name: string): void {
    delete this.defaultHeaders[name];
  }

  private async buildAgent(target: URL): Promise<http.Agent | https.Agent | undefined> {
    if (this.agent) return this.agent;

    if (this.options.proxy) {
      // Imported lazily so the dependency is only needed when a proxy is configured.
      const { SocksProxyAgent } = await import("socks-proxy-agent");
      this.agent = new SocksProxyAgent(this.options.proxy) as unknown as http.Agent;
      return this.agent;
    }

    if (target.protocol === "https:") {
      const fs = await import("node:fs");
      this.agent = new https.Agent({
        rejectUnauthorized: this.options.verifySsl !== false,
        ca: this.options.sslCaCert
          ? fs.readFileSync(this.options.sslCaCert)
          : undefined,
      });
      return this.agent;
    }

    return undefined;
  }

  /**
   * Perform a GET request and return the parsed JSON body.
   *
   * `path` is appended to the base URL verbatim, so composite application ids
   * (`{appId}/{attemptId}`) stay real path segments — the Python client achieves
   * the same by setting `safe_chars_for_path_param = "/"`.
   */
  async get<T = unknown>(
    path: string,
    query: Record<string, QueryValue> = {},
    timeoutSeconds?: number,
  ): Promise<T> {
    const url = new URL(this.baseUrl + path);
    for (const [key, value] of Object.entries(query)) {
      if (value === undefined || value === null) continue;
      url.searchParams.append(key, String(value));
    }

    const agent = await this.buildAgent(url);
    const timeoutMs = (timeoutSeconds ?? this.options.timeout ?? 30) * 1000;
    const transport = url.protocol === "https:" ? https : http;

    return new Promise<T>((resolve, reject) => {
      const request = transport.request(
        url,
        {
          method: "GET",
          agent,
          headers: { Accept: "application/json", ...this.defaultHeaders },
          timeout: timeoutMs,
        },
        (response) => {
          const chunks: Buffer[] = [];
          response.on("data", (chunk) => chunks.push(chunk as Buffer));
          response.on("end", () => {
            const body = Buffer.concat(chunks).toString("utf-8");
            const status = response.statusCode ?? 0;
            // `rawHeaders` keeps the casing the server sent, which is what
            // urllib3's HTTPHeaderDict repr shows in the Python error message.
            const headers: Record<string, string> = {};
            for (let i = 0; i < response.rawHeaders.length; i += 2) {
              const key = response.rawHeaders[i];
              const value = response.rawHeaders[i + 1];
              headers[key] = key in headers ? `${headers[key]}, ${value}` : value;
            }
            if (status < 200 || status > 299) {
              try {
                throwForStatus(status, response.statusMessage ?? "", headers, body);
              } catch (error) {
                reject(error);
              }
              return;
            }
            if (!body) {
              resolve(null as T);
              return;
            }
            try {
              resolve(parseJsonPreservingFloats(body) as T);
            } catch (error) {
              reject(
                new ApiException(
                  status,
                  response.statusMessage ?? "",
                  headers,
                  body,
                  `Failed to parse JSON response: ${(error as Error).message}`,
                ),
              );
            }
          });
        },
      );

      const poolLabel =
        `${url.protocol === "https:" ? "HTTPS" : "HTTP"}ConnectionPool` +
        `(host='${url.hostname}', port=${url.port || (url.protocol === "https:" ? 443 : 80)})`;
      const connectionLabel =
        `${url.protocol === "https:" ? "HTTPS" : "HTTP"}Connection` +
        `(host='${url.hostname}', port=${url.port || (url.protocol === "https:" ? 443 : 80)})`;

      let timedOut = false;
      request.on("timeout", () => {
        timedOut = true;
        request.destroy();
      });

      request.on("error", (error) => {
        // Reshape transport failures into the message `requests`/`urllib3`
        // produce, since the tool layer surfaces the exception text verbatim.
        if (timedOut) {
          reject(
            new OpenApiException(
              `${poolLabel}: Read timed out. (read timeout=${timeoutMs / 1000})`,
            ),
          );
          return;
        }
        const cause = describeTransportError(error as NodeJS.ErrnoException, connectionLabel);
        reject(
          new OpenApiException(
            `${poolLabel}: Max retries exceeded with url: ${url.pathname}${url.search} ` +
              `(Caused by ${cause})`,
          ),
        );
      });
      request.end();
    });
  }
}

/** Render a Node socket error the way urllib3 reports the underlying cause. */
function describeTransportError(
  error: NodeJS.ErrnoException,
  connectionLabel: string,
): string {
  const detail = (() => {
    switch (error.code) {
      case "ECONNREFUSED":
        return "[Errno 111] Connection refused";
      case "ENOTFOUND":
      case "EAI_AGAIN":
        return `[Errno -2] Name or service not known`;
      case "ECONNRESET":
        return "[Errno 104] Connection reset by peer";
      case "EHOSTUNREACH":
        return "[Errno 113] No route to host";
      case "ETIMEDOUT":
        return "[Errno 110] Connection timed out";
      default:
        return error.message;
    }
  })();

  const kind =
    error.code === "ENOTFOUND" || error.code === "EAI_AGAIN"
      ? "NameResolutionError"
      : "NewConnectionError";

  return `${kind}("${connectionLabel}: Failed to establish a new connection: ${detail}")`;
}
