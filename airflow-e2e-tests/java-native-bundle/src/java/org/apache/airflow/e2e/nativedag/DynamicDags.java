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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.*;

/**
 * Native Java Dags generated from the {@code dags.yaml} next to the bundle JAR, one per entry.
 *
 * <p>Both the Dag processor and the worker run the JAR, and each run reads the YAML again, so
 * swapping only the YAML changes the Dags with no rebuild. Without a {@code dags.yaml} next to the
 * JAR, as when the Gradle plugin runs the main class at build time or before the e2e test writes
 * one, there are no Dags, and the bundle's other Dags still parse.
 */
public class DynamicDags {
  private static final Map<String, String> GREETINGS = new HashMap<>();

  public static class Greet implements Task {
    @Override
    public void execute(Context context, Client client) {
      client.setXCom(GREETINGS.get(context.ti.dagId));
    }
  }

  public static List<DagDef> load() throws IOException, URISyntaxException {
    var jar = Path.of(DynamicDags.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    var path = jar.resolveSibling("dags.yaml");
    var dags = new ArrayList<DagDef>();
    if (!Files.isRegularFile(path)) {
      return dags;
    }
    for (var entry : new ObjectMapper(new YAMLFactory()).readTree(path.toFile()).path("dags")) {
      var dagId = entry.path("dag_id").asText();
      GREETINGS.put(dagId, entry.path("greeting").asText());
      var dag =
          new DagDef(dagId)
              .config("catchup", false)
              .config("tags", List.of("java-sdk", "java_dynamic"))
              .config("queue", QUEUE);
      dag.task("greet", Greet.class);
      dags.add(dag);
    }
    return dags;
  }
}
