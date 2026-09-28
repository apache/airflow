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
import org.apache.airflow.e2e.nativedag.AnnotationDag;
import org.apache.airflow.sdk.*;

/**
 * Bundle for the native Dag E2E tests: Dags declared entirely in Java, parsed by the Dag processor
 * from this JAR.
 *
 * <p>This is the bundle's main class, so it is the Dag source the Airflow UI shows. Every task sets
 * {@code queue}, which routes it to the {@code java-native} coordinator.
 */
public class NativeBundleBuilder {
  private static final String QUEUE = "java-native";

  public static class Extract implements Task {
    @Override
    public void execute(Context context, Client client) {
      client.setXCom(42L);
    }
  }

  public static class Transform implements Task {
    @Override
    public void execute(Context context, Client client) {
      var extracted = ((Number) client.getXCom("extract")).longValue();
      client.setXCom(extracted * 2);
    }
  }

  /** Fails the run unless the value reached it, so a successful run proves the XCom flow. */
  public static class Load implements Task {
    @Override
    public void execute(Context context, Client client) {
      var transformed = ((Number) client.getXCom("transform")).longValue();
      if (transformed != 84L) {
        throw new IllegalStateException("load expected 84 from transform, got " + transformed);
      }
    }
  }

  public static DagDef buildDag() {
    var dag =
        new DagDef("java_native_e2e")
            .config("description", "Native Java Dag of the Airflow E2E tests")
            .config("catchup", false)
            .config("tags", List.of("java-sdk", "native", "e2e"));

    var extract = dag.task("extract", Extract.class).config("queue", QUEUE);
    var transform = dag.task("transform", Transform.class).config("queue", QUEUE);
    var load = dag.task("load", Load.class).config("queue", QUEUE);

    transform.after(extract).before(load);
    return dag;
  }

  public static Bundle build() {
    return new Bundle().register(buildDag()).register(AnnotationDag.class);
  }

  public static void main(String[] args) {
    Server.create(args).serve(build());
  }
}
