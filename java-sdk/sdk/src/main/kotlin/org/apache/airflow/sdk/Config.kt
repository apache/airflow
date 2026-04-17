package org.apache.airflow.sdk

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

private const val CONFIG_FILE_NAME = "java-sdk.yaml"

open class WorkerError(
  message: String,
) : IllegalStateException(message)

class NoBody : WorkerError("No body")

/**
 * SDK configuration resolved from environment variables and an optional YAML config file.
 *
 * Resolution order (highest priority first):
 * 1. Environment variable `AIRFLOW__<SECTION>__<KEY>` (uppercase, double-underscore delimited)
 * 2. YAML config file value at `<section>.<key>` (lowercase)
 * 3. Default value (where applicable)
 *
 * Only the canonical `AIRFLOW__<SECTION>__<KEY>` env var form is recognised.
 * Single-underscore variants (`AIRFLOW_SECTION_KEY`) are **not** supported — use the
 * YAML file for a more readable alternative.
 *
 * The YAML file is loaded from `$AIRFLOW_HOME/java-sdk.yaml` when present.
 *
 * ```yaml
 * core:
 *   execution_api_server_url: "http://localhost:8080/execution/"
 *
 * sdk:
 *   bundles_dir: "./bin"
 *
 * api_auth:
 *   jwt_secret: "your-secret-key"
 *   jwt_issuer: "airflow"
 *   jwt_expiration_time: 30
 * ```
 *
 * Each YAML key corresponds directly to the env-var option name:
 * `core.execution_api_server_url` ↔ `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`.
 */
class SdkConfig(
  private val env: Map<String, String> = System.getenv(),
  yamlOverride: Path? = null,
) {
  @Suppress("UNCHECKED_CAST")
  private val yaml: Map<String, Map<String, Any?>> =
    run {
      val path = yamlOverride ?: resolveConfigPath(env)
      if (path != null && Files.isRegularFile(path)) {
        val raw = ObjectMapper(YAMLFactory()).readValue(path.toFile(), Map::class.java) as? Map<String, Any?> ?: emptyMap()
        raw.entries.associate { (k, v) ->
          k to ((v as? Map<*, *>)?.entries?.associate { (ik, iv) -> ik.toString() to iv } ?: emptyMap())
        }
      } else {
        emptyMap()
      }
    }

  /**
   * Look up a config value by section and key.
   * Checks `AIRFLOW__<SECTION>__<KEY>` env var first, then YAML `<section>.<key>`.
   */
  fun get(
    section: String,
    key: String,
  ): String? {
    val envKey = "AIRFLOW__${section.uppercase()}__${key.uppercase()}"
    env[envKey]?.takeIf { it.isNotBlank() }?.let { return it }
    return yaml[section]?.get(key)?.toString()?.takeIf { it.isNotBlank() }
  }

  /** Like [get] but throws [WorkerError] when the value is missing. */
  fun require(
    section: String,
    key: String,
  ): String =
    get(section, key)
      ?: throw WorkerError(
        "$section.$key must be configured " +
          "(AIRFLOW__${section.uppercase()}__${key.uppercase()} or $CONFIG_FILE_NAME)",
      )

  /** Resolve a positive long, falling back to [default]. */
  fun getPositiveLong(
    section: String,
    key: String,
    default: Long,
  ): Long {
    val raw = get(section, key) ?: return default
    val parsed =
      raw.toLongOrNull()
        ?: throw WorkerError("$section.$key must be an integer")
    if (parsed <= 0) throw WorkerError("$section.$key must be greater than 0")
    return parsed
  }

  // -- Execution API --

  val executionApiUrl: String
    get() {
      val url =
        get("core", "execution_api_server_url")
          ?: get("execution", "api_url")
      return url?.ensureTrailingSlash()
        ?: throw WorkerError(
          "core.execution_api_server_url must be configured " +
            "(AIRFLOW__CORE__EXECUTION_API_SERVER_URL or $CONFIG_FILE_NAME)",
        )
    }

  // -- JWT --

  val jwtSecret: String get() = require("api_auth", "jwt_secret")
  val jwtIssuer: String? get() = get("api_auth", "jwt_issuer")
  val jwtExpirationTime: Long get() = getPositiveLong("api_auth", "jwt_expiration_time", 30)

  // -- Bundle resolution --

  val bundlesDir: Path?
    get() = get("sdk", "bundles_dir")?.let(Paths::get)

  companion object {
    private fun resolveConfigPath(env: Map<String, String>): Path? {
      val home = env["AIRFLOW_HOME"]?.takeIf { it.isNotBlank() } ?: return null
      return Paths.get(home, CONFIG_FILE_NAME)
    }
  }
}

internal fun String.ensureTrailingSlash() = if (endsWith('/')) this else "$this/"
