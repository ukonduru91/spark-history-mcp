/**
 * Configuration loading for the Spark History Server MCP.
 *
 * Port of `spark_history_mcp/config/config.py` (pydantic-settings based).
 *
 * Precedence, highest first:
 *   1. Environment variables (`SHS_*`)
 *   2. `.env` file
 *   3. YAML config file (resolved by {@link resolveConfigPath})
 *   4. Built-in defaults
 */

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import YAML from "yaml";

/** Per-user config lives at ~/.config/spark-mcp/config.yaml. */
export const DEFAULT_CONFIG_FILENAME = "config.yaml";
export const APP_CONFIG_DIR = "spark-mcp";
export const ENV_VAR_PREFIX = "SHS_";

/** Direct-read vars (not Config fields); excluded from delimiter detection. */
const DIRECT_READ_VARS = new Set(["SHS_MCP_CONFIG", "SHS_MCP_TRANSPORT"]);

export interface AuthConfig {
  username?: string | null;
  password?: string | null;
  token?: string | null;
}

export interface ServerConfig {
  url?: string | null;
  auth: AuthConfig;
  default: boolean;
  verify_ssl: boolean;
  ssl_ca_cert?: string | null;
  /** EMR specific field */
  emr_cluster_arn?: string | null;
  use_proxy: boolean;
  /** HTTP request timeout in seconds */
  timeout: number;
  include_plan_description?: boolean | null;
}

export interface TransportSecurityConfig {
  enable_dns_rebinding_protection: boolean;
  allowed_hosts: string[];
  allowed_origins: string[];
}

export type TransportName = "stdio" | "streamable-http";

export interface McpConfig {
  transport?: TransportName | null;
  /** @deprecated use the singular `transport` instead. */
  transports?: TransportName[] | null;
  address?: string | null;
  port?: number | string | null;
  debug?: boolean | null;
  transport_security?: TransportSecurityConfig | null;
  /** Mirrors pydantic's `model_fields_set` for the deprecated transports fallback. */
  __fields_set__: Set<string>;
}

export interface Config {
  servers: Record<string, ServerConfig>;
  mcp: McpConfig;
}

/* ------------------------------------------------------------------ */
/* Legacy single-underscore env handling                               */
/* ------------------------------------------------------------------ */

function legacyConfigVars(): string[] {
  return Object.keys(process.env).filter(
    (env) =>
      env.startsWith(ENV_VAR_PREFIX) &&
      !DIRECT_READ_VARS.has(env) &&
      !env.includes("__"),
  );
}

export function legacyEnvMode(): boolean {
  return legacyConfigVars().length > 0;
}

function warnLegacyEnv(names: string[]): void {
  const message =
    "Single-underscore SHS_* environment variables are deprecated and will " +
    "be removed in a future major release; use the double-underscore (__) " +
    `delimiter instead. In use: ${names.join(", ")}`;
  process.emitWarning(message, "DeprecationWarning");
  console.error(`WARNING: ${message}`);
}

/* ------------------------------------------------------------------ */
/* Config file discovery                                               */
/* ------------------------------------------------------------------ */

/** Per-user config path: `$XDG_CONFIG_HOME` (if set) or `~/.config`. */
export function userConfigPath(): string {
  const xdgConfigHome =
    process.env.XDG_CONFIG_HOME || path.join(os.homedir(), ".config");
  return path.join(xdgConfigHome, APP_CONFIG_DIR, DEFAULT_CONFIG_FILENAME);
}

/**
 * Resolve which config file to load, highest precedence first.
 *
 * 1. `SHS_MCP_CONFIG` env var / `--config` flag (explicit)
 * 2. `./config.yaml`
 * 3. `~/.config/spark-mcp/config.yaml`
 *
 * Returns `[path, isExplicit]`; `path` is `null` when nothing is found (use
 * defaults) and `isExplicit` marks a missing file as fatal.
 */
export function resolveConfigPath(): [string | null, boolean] {
  const explicitPath = process.env.SHS_MCP_CONFIG;
  if (explicitPath) {
    return [explicitPath, true];
  }
  if (fs.existsSync(DEFAULT_CONFIG_FILENAME)) {
    return [DEFAULT_CONFIG_FILENAME, false];
  }
  const userPath = userConfigPath();
  if (fs.existsSync(userPath)) {
    return [userPath, false];
  }
  return [null, false];
}

/** Load and return the YAML configuration data. */
function yamlSettingsSource(): Record<string, any> {
  const [configPath, isExplicit] = resolveConfigPath();
  if (configPath === null) {
    return {};
  }
  if (!fs.existsSync(configPath)) {
    // Explicitly requested but missing -> fatal; discovered -> defaults.
    if (isExplicit) {
      const err: NodeJS.ErrnoException = new Error(
        `Config file not found: ${configPath}\n` +
          "Specified via: --config flag or SHS_MCP_CONFIG environment variable",
      );
      err.code = "ENOENT";
      throw err;
    }
    return {};
  }
  const raw = fs.readFileSync(configPath, "utf-8");
  return (YAML.parse(raw) as Record<string, any>) ?? {};
}

/* ------------------------------------------------------------------ */
/* Environment / dotenv sources                                        */
/* ------------------------------------------------------------------ */

function parseDotEnv(file: string): Record<string, string> {
  const out: Record<string, string> = {};
  if (!fs.existsSync(file)) return out;
  for (const rawLine of fs.readFileSync(file, "utf-8").split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const withoutExport = line.startsWith("export ") ? line.slice(7) : line;
    const eq = withoutExport.indexOf("=");
    if (eq === -1) continue;
    const key = withoutExport.slice(0, eq).trim();
    let value = withoutExport.slice(eq + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"') && value.length >= 2) ||
      (value.startsWith("'") && value.endsWith("'") && value.length >= 2)
    ) {
      value = value.slice(1, -1);
    }
    out[key] = value;
  }
  return out;
}

/** Coerce a raw environment string into a JSON value where appropriate. */
function coerceEnvValue(value: string): any {
  const trimmed = value.trim();
  if (trimmed.startsWith("[") || trimmed.startsWith("{")) {
    try {
      return JSON.parse(trimmed);
    } catch {
      return value;
    }
  }
  return value;
}

/**
 * Turn `SHS_A__B__C=value` pairs into a nested object, mirroring
 * pydantic-settings' `env_nested_delimiter` behaviour.
 */
function envSource(
  env: Record<string, string | undefined>,
  delimiter: string,
): Record<string, any> {
  const result: Record<string, any> = {};
  for (const [name, rawValue] of Object.entries(env)) {
    if (rawValue === undefined) continue;
    if (!name.startsWith(ENV_VAR_PREFIX)) continue;
    if (DIRECT_READ_VARS.has(name)) continue;

    const stripped = name.slice(ENV_VAR_PREFIX.length).toLowerCase();
    const parts = stripped.split(delimiter).filter((p) => p.length > 0);
    if (parts.length === 0) continue;

    let cursor = result;
    for (let i = 0; i < parts.length - 1; i++) {
      const key = parts[i];
      if (
        typeof cursor[key] !== "object" ||
        cursor[key] === null ||
        Array.isArray(cursor[key])
      ) {
        cursor[key] = {};
      }
      cursor = cursor[key];
    }
    cursor[parts[parts.length - 1]] = coerceEnvValue(rawValue);
  }
  return result;
}

function isPlainObject(value: unknown): value is Record<string, any> {
  return (
    typeof value === "object" && value !== null && !Array.isArray(value)
  );
}

/** Recursive dict merge, matching pydantic-settings' `deep_update`. */
function deepUpdate(
  base: Record<string, any>,
  override: Record<string, any>,
): Record<string, any> {
  const out: Record<string, any> = { ...base };
  for (const [key, value] of Object.entries(override)) {
    if (isPlainObject(value) && isPlainObject(out[key])) {
      out[key] = deepUpdate(out[key], value);
    } else {
      out[key] = value;
    }
  }
  return out;
}

/* ------------------------------------------------------------------ */
/* Validation / model construction                                     */
/* ------------------------------------------------------------------ */

function asBool(value: any, fallback: boolean): boolean {
  if (value === undefined || value === null) return fallback;
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  const lowered = String(value).trim().toLowerCase();
  if (["1", "true", "t", "yes", "y", "on"].includes(lowered)) return true;
  if (["0", "false", "f", "no", "n", "off"].includes(lowered)) return false;
  throw new Error(`Input should be a valid boolean, got ${JSON.stringify(value)}`);
}

function asOptionalBool(value: any): boolean | null {
  if (value === undefined || value === null) return null;
  return asBool(value, false);
}

function asInt(value: any, fallback: number): number {
  if (value === undefined || value === null) return fallback;
  const parsed = typeof value === "number" ? value : Number(String(value).trim());
  if (!Number.isFinite(parsed)) {
    throw new Error(`Input should be a valid integer, got ${JSON.stringify(value)}`);
  }
  return Math.trunc(parsed);
}

function asOptionalString(value: any): string | null {
  if (value === undefined || value === null) return null;
  return String(value);
}

function asStringList(value: any): string[] {
  if (value === undefined || value === null) return [];
  if (Array.isArray(value)) return value.map((v) => String(v));
  return [String(value)];
}

function buildAuth(raw: any): AuthConfig {
  const data = isPlainObject(raw) ? raw : {};
  return {
    username: asOptionalString(data.username),
    password: asOptionalString(data.password),
    token: asOptionalString(data.token),
  };
}

function buildServer(raw: any): ServerConfig {
  const data = isPlainObject(raw) ? raw : {};
  return {
    url: asOptionalString(data.url),
    auth: buildAuth(data.auth),
    default: asBool(data.default, false),
    verify_ssl: asBool(data.verify_ssl, true),
    ssl_ca_cert: asOptionalString(data.ssl_ca_cert),
    emr_cluster_arn: asOptionalString(data.emr_cluster_arn),
    use_proxy: asBool(data.use_proxy, false),
    timeout: asInt(data.timeout, 30),
    include_plan_description: asOptionalBool(data.include_plan_description),
  };
}

function buildTransportSecurity(raw: any): TransportSecurityConfig | null {
  if (!isPlainObject(raw)) return null;
  return {
    enable_dns_rebinding_protection: asBool(
      raw.enable_dns_rebinding_protection,
      false,
    ),
    allowed_hosts: asStringList(raw.allowed_hosts),
    allowed_origins: asStringList(raw.allowed_origins),
  };
}

const VALID_TRANSPORTS: TransportName[] = ["stdio", "streamable-http"];

function asTransport(value: any): TransportName | null {
  if (value === undefined || value === null) return null;
  const name = String(value);
  if (!VALID_TRANSPORTS.includes(name as TransportName)) {
    throw new Error(
      `Input should be 'stdio' or 'streamable-http', got ${JSON.stringify(value)}`,
    );
  }
  return name as TransportName;
}

function buildMcp(raw: any): McpConfig {
  const data = isPlainObject(raw) ? raw : {};
  const fieldsSet = new Set(Object.keys(data));
  return {
    transport: asTransport(data.transport),
    transports:
      data.transports === undefined || data.transports === null
        ? null
        : asStringList(data.transports).map((t) => asTransport(t)!),
    address: data.address === undefined ? "localhost" : asOptionalString(data.address),
    port: data.port === undefined ? "18888" : (data.port as number | string | null),
    debug: data.debug === undefined ? false : asOptionalBool(data.debug),
    transport_security: buildTransportSecurity(data.transport_security),
    __fields_set__: fieldsSet,
  };
}

function buildConfig(data: Record<string, any>): Config {
  const serversRaw = isPlainObject(data.servers) ? data.servers : undefined;

  let servers: Record<string, ServerConfig>;
  if (serversRaw === undefined) {
    // Field default.
    servers = {
      local: buildServer({ url: "http://localhost:18080", default: true }),
    };
  } else {
    servers = {};
    for (const [name, value] of Object.entries(serversRaw)) {
      servers[name] = buildServer(value);
    }
  }

  return {
    servers,
    mcp: buildMcp(data.mcp === undefined ? {} : data.mcp),
  };
}

/* ------------------------------------------------------------------ */
/* Public entry point                                                  */
/* ------------------------------------------------------------------ */

function assembleSettings(delimiter: string): Config {
  // Lowest precedence first; later sources win.
  const yamlData = yamlSettingsSource();
  const dotenvData = envSource(parseDotEnv(".env"), delimiter);
  const envData = envSource(process.env, delimiter);

  let merged: Record<string, any> = {};
  merged = deepUpdate(merged, yamlData);
  merged = deepUpdate(merged, dotenvData);
  merged = deepUpdate(merged, envData);

  return buildConfig(merged);
}

/** Build the configuration, falling back to the legacy delimiter when in use. */
export function loadConfig(): Config {
  if (legacyEnvMode()) {
    warnLegacyEnv(legacyConfigVars());
    return assembleSettings("_");
  }
  return assembleSettings("__");
}

/** JSON-ish dump used for the debug log line, mirroring `model_dump_json`. */
export function dumpConfig(config: Config): Record<string, any> {
  const servers: Record<string, any> = {};
  for (const [name, server] of Object.entries(config.servers)) {
    // `auth` is excluded from the dump in the Python model (exclude=True).
    const { auth: _auth, ...rest } = server;
    servers[name] = rest;
  }
  const { __fields_set__: _fieldsSet, ...mcp } = config.mcp;
  return { servers, mcp };
}
