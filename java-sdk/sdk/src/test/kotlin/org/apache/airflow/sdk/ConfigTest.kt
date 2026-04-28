/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.airflow.sdk

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

class ConfigTest {
  // -- SdkConfig: env var resolution --

  @Test
  @DisplayName("executionApiUrl uses AIRFLOW__CORE__EXECUTION_API_SERVER_URL env var")
  fun executionApiUrlFromEnv() {
    val config = SdkConfig(env = mapOf("AIRFLOW__CORE__EXECUTION_API_SERVER_URL" to "http://127.0.0.1:8080/execution"))

    assertEquals("http://127.0.0.1:8080/execution/", config.executionApiUrl)
  }

  @Test
  @DisplayName("executionApiUrl throws when missing")
  fun executionApiUrlThrowsWhenMissing() {
    val config = SdkConfig(env = emptyMap())

    val error = assertThrows(WorkerError::class.java) { config.executionApiUrl }
    assertTrue(error.message!!.contains("execution_api_server_url"))
  }

  @Test
  @DisplayName("executionApiUrl falls back to execution.api_url")
  fun executionApiUrlFallback() {
    val config = SdkConfig(env = mapOf("AIRFLOW__EXECUTION__API_URL" to "http://127.0.0.1:8080/execution"))

    assertEquals("http://127.0.0.1:8080/execution/", config.executionApiUrl)
  }

  @Test
  @DisplayName("jwtExpirationTime defaults to 30 seconds")
  fun jwtExpirationTimeDefault() {
    val config = SdkConfig(env = emptyMap())

    assertEquals(30, config.jwtExpirationTime)
  }

  // -- SdkConfig: YAML resolution --

  @Test
  @DisplayName("config values are loaded from YAML file")
  fun yamlConfigLoading(
    @TempDir tempDir: Path,
  ) {
    val yamlContent =
      """
      core:
        execution_api_server_url: "http://yaml-host:8080/execution/"

      sdk:
        bundles_dir: "./bundles"

      api_auth:
        jwt_secret: "yaml-secret"
        jwt_issuer: "yaml-issuer"
        jwt_expiration_time: 45
      """.trimIndent()

    val yamlPath = tempDir.resolve("java-sdk.yaml")
    Files.writeString(yamlPath, yamlContent)

    val config = SdkConfig(env = emptyMap(), yamlOverride = yamlPath)

    assertEquals("http://yaml-host:8080/execution/", config.executionApiUrl)
    assertEquals("yaml-secret", config.jwtSecret)
    assertEquals("yaml-issuer", config.jwtIssuer)
    assertEquals(45, config.jwtExpirationTime)
    assertEquals(Path.of("./bundles"), config.bundlesDir)
  }

  @Test
  @DisplayName("env vars take precedence over YAML values")
  fun envTakesPrecedenceOverYaml(
    @TempDir tempDir: Path,
  ) {
    val yamlContent =
      """
      core:
        execution_api_server_url: "http://yaml-host:8080/execution/"
      api_auth:
        jwt_secret: "yaml-secret"
      """.trimIndent()

    val yamlPath = tempDir.resolve("java-sdk.yaml")
    Files.writeString(yamlPath, yamlContent)

    val config =
      SdkConfig(
        env =
          mapOf(
            "AIRFLOW__CORE__EXECUTION_API_SERVER_URL" to "http://env-host:9090/execution/",
            "AIRFLOW__API_AUTH__JWT_SECRET" to "env-secret",
          ),
        yamlOverride = yamlPath,
      )

    assertEquals("http://env-host:9090/execution/", config.executionApiUrl)
    assertEquals("env-secret", config.jwtSecret)
  }

  @Test
  @DisplayName("config works with no YAML file and no env vars for optional values")
  fun noYamlFile() {
    val config = SdkConfig(env = emptyMap())

    assertEquals(30, config.jwtExpirationTime)
    assertNull(config.bundlesDir)
  }

  @Test
  @DisplayName("YAML file is resolved from AIRFLOW_HOME")
  fun yamlFromAirflowHome(
    @TempDir tempDir: Path,
  ) {
    val yamlContent =
      """
      api_auth:
        jwt_secret: "home-secret"
      """.trimIndent()

    Files.writeString(tempDir.resolve("java-sdk.yaml"), yamlContent)

    val config = SdkConfig(env = mapOf("AIRFLOW_HOME" to tempDir.toString()))

    assertEquals("home-secret", config.jwtSecret)
  }
}
