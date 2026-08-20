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
 * Bundle of deliberately broken task classes for the runner-behaviour E2E tests.
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

  @NotNull
  @Override
  public Iterable<Dag> getDags() {
    var dag = new Dag("java_uninstantiable");
    dag.addTask("missing_no_arg_constructor", MissingNoArgConstructor.class);
    dag.addTask("non_static_inner", NonStaticInner.class);
    return List.of(dag);
  }

  public static void main(String[] args) {
    var bundle = new TestBundleBuilder().build();
    Server.create(args).serve(bundle);
  }
}
