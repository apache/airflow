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

package org.apache.airflow.example

import org.apache.airflow.sdk.DagDef

// Lives outside org.apache.airflow.sdk, as user code does, so the SDK does not skip its frames.
class DagSourceFixtures {
  fun plain() = DagDef("plain")

  fun fromLambda(): DagDef {
    lateinit var dag: DagDef
    Runnable { dag = DagDef("lambda") }.run()
    return dag
  }

  fun fromAnonymous(): DagDef {
    lateinit var dag: DagDef
    object : Runnable {
      override fun run() {
        dag = DagDef("anonymous")
      }
    }.run()
    return dag
  }

  class Nested {
    fun make() = DagDef("nested")

    class Deeper {
      fun make() = DagDef("deeper")
    }
  }
}
