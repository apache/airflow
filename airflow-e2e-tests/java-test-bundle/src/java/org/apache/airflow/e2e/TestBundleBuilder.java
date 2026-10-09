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
import java.util.Map;
import java.util.Objects;
import org.apache.airflow.sdk.*;
import org.jetbrains.annotations.NotNull;

/**
 * Bundle for the runner-behaviour E2E tests: deliberately broken task classes that exercise
 * instantiation failures, and tasks that round-trip Airflow Variables and asset state through the
 * supervisor.
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

  /**
   * Calls every asset state store method on one asset, through a lookup by name and a lookup by
   * URI. Each step changes the state through one lookup and checks the result through the other,
   * so the task fails unless both lookups reach the same asset. The E2E test then reads the
   * remaining keys through the REST API.
   */
  public static class UseAssetStateStore implements Task {
    public void execute(@NotNull Context context, Client client) {
      var byName = client.getAssetStateStore().byName("java_e2e_orders");
      var byUri = client.getAssetStateStore().byUri("x-java-e2e://orders");

      byName.set("scratch", "first");
      expect("first", byUri.get("scratch"), "set() through the name");
      byUri.clear();
      expect(null, byName.get("scratch"), "clear() through the URI");

      byUri.set("scratch", "second");
      expect("second", byName.get("scratch"), "set() through the URI");
      byName.clear();
      expect(null, byUri.get("scratch"), "clear() through the name");

      byName.set("deleted", "temporary");
      byUri.delete("deleted");
      expect(null, byName.get("deleted"), "delete() through the URI");
      byUri.set("deleted", "temporary");
      byName.delete("deleted");
      expect(null, byUri.get("deleted"), "delete() through the name");

      var summary = Map.of("run_id", context.dagRun.runId, "rows", 3L);
      byName.set("summary", summary);
      expect(summary, byUri.get("summary"), "set() through the name, with a map value");
    }

    private static void expect(Object expected, Object actual, String step) {
      if (!Objects.equals(expected, actual)) {
        throw new IllegalStateException(step + ": expected " + expected + " but read " + actual);
      }
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
    var assetStateStore = new DagDef("java_asset_state_store");
    assetStateStore.addTask("use_asset_state_store", UseAssetStateStore.class);
    return List.of(uninstantiable, variableWrite, assetStateStore);
  }

  public static void main(String[] args) {
    var bundle = new TestBundleBuilder().build();
    Server.create(args).serve(bundle);
  }
}
