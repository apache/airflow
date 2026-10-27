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

/** A native Java Dag declared with annotations; it sets the same queue as the interface Dag. */
@Airflow.Dag(
    id = "java_native_annotation_e2e",
    description = "Native Java Dag of the Airflow E2E tests, declared with annotations",
    catchup = false,
    queue = QUEUE,
    tags = {"java-sdk", "native", "e2e"})
public class AnnotationDag {
  @Airflow.Task(id = "extract")
  public long extract() {
    return 42L;
  }

  @Airflow.Task(id = "transform")
  public long transform(long extracted, double factor) {
    return (long) (extracted * factor);
  }

  /** Fails the run unless the value reached it, so a successful run proves the XCom flow. */
  @Airflow.Task(id = "load")
  public void load(long transformed) {
    if (transformed != 63L) {
      throw new IllegalStateException("load expected 63 from transform, got " + transformed);
    }
  }

  @Airflow.Task(id = "report_many")
  public void reportMany() {}

  @Airflow.Task(id = "report_few")
  public void reportFew() {}

  /** Picks one of {@link #reportMany} and {@link #reportFew}; the other is skipped. */
  @Airflow.If(id = "has_rows")
  public boolean hasRows(long transformed) {
    return transformed > 0;
  }

  @Airflow.Task(id = "report_long")
  public void reportLong() {}

  @Airflow.Task(id = "report_short")
  public void reportShort() {}

  /** Names the one of {@link #reportLong} and {@link #reportShort} that runs; the other is skipped. */
  @Airflow.Switch(id = "pick_report")
  public Class<? extends Task> pickReport(long transformed) {
    return transformed > 100 ? AnnotationDagBuilder.ReportLong.class : AnnotationDagBuilder.ReportShort.class;
  }

  // A task that starts a run of another Dag. The method runs when this Dag is
  // built, not when the task runs, so it takes no arguments.
  @Airflow.Task(id = "trigger_downstream")
  public TriggerDagRun triggerDownstream() {
    return new TriggerDagRun("java_native_target_e2e");
  }

  // A task group: everything it declares is prefixed with its id, so this is
  // the task "checks.audit".
  @Airflow.TaskGroup(id = "checks")
  static class Checks {
    @Airflow.Task(id = "audit")
    public void audit() {}
  }

  @Airflow.Deps
  static class Wiring implements AnnotationDagDeps {
    void depends() {
      var extracted = extract();
      var transformed = transform(extracted, lit(1.5));
      var loaded = load(transformed);
      hasRows(transformed).Then(reportMany()).Else(reportFew());
      pickReport(transformed).Case(reportLong()).Case(reportShort());
      loaded.before(triggerDownstream());
      // Ordering-only edge: the checks group runs after extract, with no data
      // flowing.
      extracted.before(checks());
      checks().audit();
    }
  }
}
