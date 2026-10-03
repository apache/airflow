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

// "native" is a Java keyword, so the native Dags live in "nativedag".
package org.apache.airflow.e2e.nativedag;

import static org.apache.airflow.e2e.NativeBundleBuilder.QUEUE;

import org.apache.airflow.sdk.*;

/** A native Java Dag declared with annotations; its tasks use the interface Dag's queue. */
@Builder.Dag(
    id = "java_native_annotation_e2e",
    description = "Native Java Dag of the Airflow E2E tests, declared with annotations",
    catchup = false,
    tags = {"java-sdk", "native", "e2e"})
public class AnnotationDag {
  @Builder.Task(id = "extract", queue = QUEUE)
  public long extract() {
    return 42L;
  }

  @Builder.Task(id = "transform", queue = QUEUE)
  public long transform(long extracted, double factor) {
    return (long) (extracted * factor);
  }

  /** Fails the run unless the value reached it, so a successful run proves the XCom flow. */
  @Builder.Task(id = "load", queue = QUEUE)
  public void load(long transformed) {
    if (transformed != 63L) {
      throw new IllegalStateException("load expected 63 from transform, got " + transformed);
    }
  }

  @Builder.Task(id = "audit", queue = QUEUE)
  public void audit() {}

  @Builder.Deps
  static class Wiring implements AnnotationDagDeps {
    void depends() {
      var extracted = extract();
      load(transform(extracted, lit(1.5)));
      // Ordering-only edge: audit runs after extract, with no data flowing.
      extracted.before(audit());
    }
  }
}
