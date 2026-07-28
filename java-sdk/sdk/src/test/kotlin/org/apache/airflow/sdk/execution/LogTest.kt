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

package org.apache.airflow.sdk.execution

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class LogTest {
  @AfterEach
  fun tearDown() {
    LogCapture.resetThresholds()
  }

  @Test
  @DisplayName("Without overrides, a logger is gated by the global threshold")
  fun shouldUseGlobalThreshold() {
    Log.globalThreshold = Level.INFO
    Log.namedThresholds = emptyMap()

    assertTrue(Log.isEnabledForLevel(Level.WARNING, "any"))
    assertTrue(Log.isEnabledForLevel(Level.INFO, "any"))
    assertFalse(Log.isEnabledForLevel(Level.DEBUG, "any"))
  }

  @Test
  @DisplayName("An exact namespace override applies to its logger; unrelated loggers use global")
  fun shouldApplyNamespaceOverride() {
    Log.globalThreshold = Level.INFO
    Log.namedThresholds = mapOf("chatty" to Level.DEBUG)

    assertTrue(Log.isEnabledForLevel(Level.DEBUG, "chatty"))
    assertFalse(Log.isEnabledForLevel(Level.DEBUG, "other"))
    assertTrue(Log.isEnabledForLevel(Level.INFO, "other"))
  }

  @Test
  @DisplayName("A nested logger inherits the level of its nearest configured ancestor")
  fun shouldInheritFromAncestor() {
    Log.globalThreshold = Level.INFO
    Log.namedThresholds = mapOf("foo" to Level.WARNING)

    // foo.bar and foo.bar.rex have no exact entry, so they inherit foo=WARNING.
    assertFalse(Log.isEnabledForLevel(Level.INFO, "foo.bar"))
    assertFalse(Log.isEnabledForLevel(Level.INFO, "foo.bar.rex"))
    assertTrue(Log.isEnabledForLevel(Level.WARNING, "foo.bar.rex"))
  }

  @Test
  @DisplayName("The most specific configured ancestor wins")
  fun shouldPreferMostSpecificAncestor() {
    Log.globalThreshold = Level.INFO
    Log.namedThresholds = mapOf("foo" to Level.WARNING, "foo.bar" to Level.DEBUG)

    // foo.bar.rex's nearest ancestor is foo.bar=DEBUG, not foo=WARNING.
    assertTrue(Log.isEnabledForLevel(Level.DEBUG, "foo.bar.rex"))
    // A sibling under foo (but not under foo.bar) still resolves to foo=WARNING.
    assertFalse(Log.isEnabledForLevel(Level.INFO, "foo.baz"))
  }

  @Test
  @DisplayName("Inheritance respects dotted-segment boundaries, not raw string prefixes")
  fun shouldNotMatchPartialSegmentPrefixes() {
    Log.globalThreshold = Level.INFO
    Log.namedThresholds = mapOf("foo" to Level.WARNING)

    // "foobar" is not a child of "foo"; it must fall back to the global level.
    assertTrue(Log.isEnabledForLevel(Level.INFO, "foobar"))
  }
}
