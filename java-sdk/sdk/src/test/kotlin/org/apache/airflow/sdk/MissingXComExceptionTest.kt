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

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class MissingXComExceptionTest {
  @Test
  @DisplayName("requireXCom returns the value when the XCom is present")
  fun requireXComReturnsPresentValue() {
    Assertions.assertEquals(7L, MissingXComException.requireXCom(7L, "produce", "value"))
  }

  @Test
  @DisplayName("requireXCom throws naming the task and parameter when the XCom is absent")
  fun requireXComThrowsWhenAbsent() {
    val ex =
      Assertions.assertThrows(MissingXComException::class.java) {
        MissingXComException.requireXCom(null, "produce", "value")
      }
    Assertions.assertTrue("produce" in ex.message!!)
    Assertions.assertTrue("value" in ex.message!!)
  }
}
