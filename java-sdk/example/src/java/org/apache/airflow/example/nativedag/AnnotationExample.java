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

// "native" is a Java keyword, so the native-Dag examples live in "nativedag".
package org.apache.airflow.example.nativedag;

import static java.lang.System.Logger.Level.INFO;

import org.apache.airflow.sdk.*;

// A Dag defined entirely in Java, annotation-style: no Python stub file
// describes it. The @Builder.Dag/@Builder.Task attributes carry the
// configuration and the @Builder.Deps class declares the graph -- the single
// place dependencies are defined.
@Builder.Dag(
    id = "java_native_annotation_example",
    description = "Pure-Java Dag authored with annotations, without a Python stub file",
    schedule = "@daily",
    startDate = "2026-01-01T00:00:00Z",
    catchup = false,
    tags = {"example", "java-sdk"})
public class AnnotationExample {
  private static final System.Logger log = System.getLogger(AnnotationExample.class.getName());

  @Builder.Task(id = "extract", retries = 2)
  public long extract() {
    log.log(INFO, "Extracting a value");
    return 42L;
  }

  @Builder.Task(id = "transform")
  public long transform(long extracted, double factor) {
    log.log(INFO, "Transforming {0} by {1}", extracted, factor);
    return (long) (extracted * factor);
  }

  @Builder.Task(id = "load")
  public void load(long transformed) {
    log.log(INFO, "Loaded {0}", transformed);
  }

  @Builder.Task(id = "audit")
  public void audit() {
    log.log(INFO, "Audited the run");
  }

  // Implements the generated wiring view, so javac type-checks the graph:
  // extract() yields a TaskRef<Long>, which is an Arg<Long> for transform.
  @Builder.Deps
  static class Wiring implements AnnotationExampleDeps {
    void depends() {
      var extracted = extract();
      load(transform(extracted, lit(1.5)));
      // Ordering-only edge: audit runs after extract, with no data flowing.
      extracted.before(audit());
    }
  }
}
