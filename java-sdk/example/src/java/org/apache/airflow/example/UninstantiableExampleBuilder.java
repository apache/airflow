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

package org.apache.airflow.example;

import org.apache.airflow.sdk.*;
import org.jetbrains.annotations.NotNull;

public class UninstantiableExampleBuilder {
  public static class MissingNoArgConstructor implements Task {
    private final String marker;

    public MissingNoArgConstructor(String marker) {
      this.marker = marker;
    }

    public void execute(@NotNull Context context, Client client) {
      throw new IllegalStateException(marker);
    }
  }

  /**
   * The not static only constructor implicitly takes the enclosing instance, so the
   * runner's no-argument lookup fails even though no constructor is declared.
   */
  public class NonStaticInner implements Task {
    public void execute(@NotNull Context context, Client client) {}
  }

  public static Dag build() {
    var dag = new Dag("java_uninstantiable_example");
    dag.addTask("missing_no_arg_constructor", MissingNoArgConstructor.class);
    dag.addTask("non_static_inner", NonStaticInner.class);
    return dag;
  }
}
