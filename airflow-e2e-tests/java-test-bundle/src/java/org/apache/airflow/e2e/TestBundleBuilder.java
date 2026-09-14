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

package org.apache.airflow.e2e;

import java.util.List;
import org.apache.airflow.sdk.*;
import org.jetbrains.annotations.NotNull;

/**
 * Bundle for the runner-behaviour E2E tests: deliberately broken task classes that exercise
 * instantiation failures, and a task that round-trips Airflow Variables through the supervisor.
 */
public class TestBundleBuilder implements BundleBuilder {
  public static class MissingNoArgConstructor implements Task {
    public MissingNoArgConstructor(String unused) {}

    public void execute(@NotNull Context context, Client client) {
      throw new IllegalStateException("should not be reachable");
    }
  }

  /**
   * A non-static nested class declares no constructor of its own, but the implicit one
   * takes the enclosing instance, so the runner's lookup for a no-argument constructor
   * fails.
   */
  public class NonStaticInner implements Task {
    public void execute(@NotNull Context context, Client client) {
      throw new IllegalStateException("should not be reachable");
    }
  }

  /**
   * Stores this run's id where the E2E test can read it back through the REST API, then writes
   * and deletes a scratch variable to exercise the delete path.
   */
  public static class WriteAndDeleteVariable implements Task {
    public void execute(@NotNull Context context, Client client) {
      client.setVariable(
          "java_e2e_variable", context.dagRun.runId, "written by the Java SDK e2e test");
      client.setVariable("java_e2e_scratch", "scratch");
      client.deleteVariable("java_e2e_scratch");
    }
  }

  @NotNull
  @Override
  public Iterable<DagDef> getDags() {
    var uninstantiable = new DagDef("java_uninstantiable");
    uninstantiable.addTask("missing_no_arg_constructor", MissingNoArgConstructor.class);
    uninstantiable.addTask("non_static_inner", NonStaticInner.class);
    var variableWrite = new DagDef("java_variable_write");
    variableWrite.addTask("write_and_delete", WriteAndDeleteVariable.class);
    return List.of(uninstantiable, variableWrite);
  }

  public static void main(String[] args) {
    var bundle = new TestBundleBuilder().build();
    Server.create(args).serve(bundle);
  }
}
