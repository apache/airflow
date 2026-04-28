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

/**
 * Interface for declaring DAGs in a bundle.
 *
 * <p>Implement this interface in the class specified as {@code Main-Class} in your JAR manifest.
 * The build system instantiates this class at compile time to extract dag_ids and task_ids
 * into the JAR manifest, enabling inspection of bundled DAGs without running the full process.
 */
interface DagBundle {
  fun getDags(): List<Dag>
}
