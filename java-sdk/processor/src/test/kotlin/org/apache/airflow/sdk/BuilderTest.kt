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

import com.google.testing.compile.CompilationSubject.assertThat
import com.google.testing.compile.Compiler
import com.google.testing.compile.JavaFileObjectSubject
import com.google.testing.compile.JavaFileObjects
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

private fun compile(source: String) =
  Compiler.javac().withProcessors(BuilderProcessor()).compile(
    JavaFileObjects.forSourceString("org.apache.airflow.example.TestExample", source),
  )

private fun JavaFileObjectSubject.hasSourceEquivalentTo(
  qual: String,
  source: String,
) = hasSourceEquivalentTo(
  JavaFileObjects.forSourceString(qual, source),
)

class BuilderTest {
  @Test
  @DisplayName("generate builder and task-reference twins for dag class")
  fun generateBuilderAndRefForDagClass() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;

        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.Client;
        import org.apache.airflow.sdk.Context;
        import org.apache.airflow.sdk.Wiring;

        @Builder.Dag
        public class TestExample {
          @Builder.Task
          public void t1() {}

          @Builder.Task
          public int t2(Client client) {
            return 7;
          }

          @Builder.Task
          public void t3(Context ctx, int value) {
            System.out.println(String.format("%s %s", ctx.ti, value));
          }

          @Wiring
          static void depends(TestExampleRef f) {
            f.t1();
            f.t3(f.t2());
          }
        }
      """,
      )

    assertThat(compilation).succeeded()
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleBuilder")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleBuilder",
        """
         package org.apache.airflow.example;

         import java.lang.Exception;
         import java.lang.Integer;
         import java.lang.Override;
         import java.lang.String;
         import java.util.List;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.internal.ArgValues;
         import org.apache.airflow.sdk.internal.Refs;

         public final class TestExampleBuilder {
           public static final String DAG_ID = "TestExample";

           public static DagDef dag() {
             var dag = new DagDef(DAG_ID);
             return dag;
           }

           public static DagDef build() {
             var dag = dag();
             TestExample.depends(new TestExampleRef(dag));
             Refs.requireRegistered(dag, List.of("t1", "t2", "t3"));
             return dag;
           }

           public static final class T1 implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               new TestExample().t1();
             }
           }
           public static final class T2 implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               client.setXCom(new TestExample().t2(client));
             }
           }
           public static final class T3 implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               int value = ArgValues.requiredInput(context, client, 0, Integer.class, "value");
               new TestExample().t3(context, value);
             }
           }
         }
        """,
      )
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleRef")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleRef",
        """
         package org.apache.airflow.example;

         import java.lang.Integer;
         import java.lang.Number;
         import java.lang.Void;
         import java.util.List;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.In;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.TaskRef;
         import org.apache.airflow.sdk.internal.Refs;

         public final class TestExampleRef {
           private final DagDef dag;

           public TestExampleRef(DagDef dag) {
             this.dag = dag;
           }

           public TaskRef<Void> t1() {
             return Refs.register(dag, new TaskDef("t1", TestExampleBuilder.T1.class), List.of());
           }

           public TaskRef<Integer> t2() {
             return Refs.register(dag, new TaskDef("t2", TestExampleBuilder.T2.class), List.of());
           }

           public TaskRef<Void> t3(In<? extends Number> value) {
             return Refs.register(dag, new TaskDef("t3", TestExampleBuilder.T3.class), List.of(value));
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("bind data parameters by position, skipping the injected Client and Context")
  fun generateBuilderBindsDataParametersByPosition() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.Client;
        import org.apache.airflow.sdk.Context;
        @Builder.Dag
        public class TestExample {
          @Builder.Task
          public void t(long first, Client client, String second, Context ctx, Integer third) {}
        }
      """,
      )

    assertThat(compilation).succeeded()
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleBuilder")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleBuilder",
        """
         package org.apache.airflow.example;

         import java.lang.Exception;
         import java.lang.Integer;
         import java.lang.Long;
         import java.lang.Override;
         import java.lang.String;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.internal.ArgValues;

         public final class TestExampleBuilder {
           public static final String DAG_ID = "TestExample";

           public static DagDef dag() {
             var dag = new DagDef(DAG_ID);
             return dag;
           }

           public static DagDef build() {
             var dag = dag();
             dag.addTask(new TaskDef("t", T.class));
             return dag;
           }

           public static final class T implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               long first = ArgValues.requiredInput(context, client, 0, Long.class, "first");
               String second = ArgValues.optionalInput(context, client, 1, String.class);
               Integer third = ArgValues.optionalInput(context, client, 2, Integer.class);
               new TestExample().t(first, client, second, context, third);
             }
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("require primitive parameters, leave boxed and parameterized types nullable")
  fun generateBuilderRequiresPrimitivesOnly() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import java.util.List;
        import java.util.Map;
        import org.apache.airflow.sdk.Builder;
        @Builder.Dag
        public class TestExample {
          @Builder.Task
          public void t(boolean flag, float fraction, Double boxed, List<String> tags, Map raw) {}
        }
      """,
      )

    assertThat(compilation).succeeded()
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleBuilder")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleBuilder",
        """
         package org.apache.airflow.example;

         import java.lang.Boolean;
         import java.lang.Double;
         import java.lang.Exception;
         import java.lang.Float;
         import java.lang.Override;
         import java.lang.String;
         import java.util.List;
         import java.util.Map;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.internal.ArgValues;

         public final class TestExampleBuilder {
           public static final String DAG_ID = "TestExample";

           public static DagDef dag() {
             var dag = new DagDef(DAG_ID);
             return dag;
           }

           public static DagDef build() {
             var dag = dag();
             dag.addTask(new TaskDef("t", T.class));
             return dag;
           }

           public static final class T implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               boolean flag = ArgValues.requiredInput(context, client, 0, Boolean.class, "flag");
               float fraction = ArgValues.requiredInput(context, client, 1, Float.class, "fraction");
               Double boxed = ArgValues.optionalInput(context, client, 2, Double.class);
               List<String> tags = (List<String>) ArgValues.optionalInput(context, client, 3, List.class);
               Map raw = ArgValues.optionalInput(context, client, 4, Map.class);
               new TestExample().t(flag, fraction, boxed, tags, raw);
             }
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("type twin inputs by declared parameter type")
  fun generateRefTypesTwinInputs() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import java.util.List;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.Wiring;
        @Builder.Dag
        public class TestExample {
          @Builder.Task
          public String ps() { return "x"; }

          @Builder.Task
          public void pv() {}

          @Builder.Task
          public List<String> pl() { return null; }

          @Builder.Task
          public long pn() { return 1L; }

          @Builder.Task
          public void t(String text, Object anything, List<String> items, Integer boxed) {}

          @Wiring
          static void depends(TestExampleRef f) {
            f.t(f.ps(), f.pv(), f.pl(), f.pn());
          }
        }
      """,
      )

    assertThat(compilation).succeeded()
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleRef")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleRef",
        """
         package org.apache.airflow.example;

         import java.lang.Long;
         import java.lang.Number;
         import java.lang.String;
         import java.lang.Void;
         import java.util.List;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.In;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.TaskRef;
         import org.apache.airflow.sdk.internal.Refs;

         public final class TestExampleRef {
           private final DagDef dag;

           public TestExampleRef(DagDef dag) {
             this.dag = dag;
           }

           public TaskRef<String> ps() {
             return Refs.register(dag, new TaskDef("ps", TestExampleBuilder.Ps.class), List.of());
           }

           public TaskRef<Void> pv() {
             return Refs.register(dag, new TaskDef("pv", TestExampleBuilder.Pv.class), List.of());
           }

           public TaskRef<List<String>> pl() {
             return Refs.register(dag, new TaskDef("pl", TestExampleBuilder.Pl.class), List.of());
           }

           public TaskRef<Long> pn() {
             return Refs.register(dag, new TaskDef("pn", TestExampleBuilder.Pn.class), List.of());
           }

           public TaskRef<Void> t(In<? extends String> text, In<?> anything,
               In<? extends List<String>> items, In<? extends Number> boxed) {
             return Refs.register(dag, new TaskDef("t", TestExampleBuilder.T.class), List.of(text, anything, items, boxed));
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("lower explicit annotation attributes into config calls")
  fun generateBuilderLowersConfigAttributes() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.Wiring;
        @Builder.Dag(id = "cfg", schedule = "@daily", tags = {"a", "b"}, catchup = true,
            startDate = "2026-01-01T00:00:00Z")
        public class TestExample {
          @Builder.Task(retries = 2, queue = "q", retryDelay = "PT5M", retryExponentialBackoff = 1.5)
          public void t1() {}

          @Wiring
          static void depends(TestExampleRef f) {
            f.t1();
          }
        }
      """,
      )

    assertThat(compilation).succeeded()
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleBuilder")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleBuilder",
        """
         package org.apache.airflow.example;

         import java.lang.Exception;
         import java.lang.Override;
         import java.lang.String;
         import java.time.OffsetDateTime;
         import java.util.List;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.internal.Refs;

         public final class TestExampleBuilder {
           public static final String DAG_ID = "cfg";

           public static DagDef dag() {
             var dag = new DagDef(DAG_ID);
             dag.config("schedule", "@daily");
             dag.config("tags", List.of("a", "b"));
             dag.config("catchup", true);
             dag.config("start_date", OffsetDateTime.parse("2026-01-01T00:00:00Z"));
             return dag;
           }

           public static DagDef build() {
             var dag = dag();
             TestExample.depends(new TestExampleRef(dag));
             Refs.requireRegistered(dag, List.of("t1"));
             return dag;
           }

           public static final class T1 implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               new TestExample().t1();
             }
           }
         }
        """,
      )
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleRef")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleRef",
        """
         package org.apache.airflow.example;

         import java.lang.Void;
         import java.time.Duration;
         import java.util.List;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.TaskRef;
         import org.apache.airflow.sdk.internal.Refs;

         public final class TestExampleRef {
           private final DagDef dag;

           public TestExampleRef(DagDef dag) {
             this.dag = dag;
           }

           public TaskRef<Void> t1() {
             return Refs.register(dag, new TaskDef("t1", TestExampleBuilder.T1.class).config("retries", 2).config("queue", "q").config("retry_delay", Duration.parse("PT5M")).config("retry_exponential_backoff", 1.5), List.of());
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("bind an input bundle through the shared populator")
  fun generateBuilderBindsInputBundleFields() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import java.util.List;
        import org.apache.airflow.sdk.ArgName;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.Client;
        import org.apache.airflow.sdk.In;
        import org.apache.airflow.sdk.TaskInput;
        import org.apache.airflow.sdk.Wiring;
        @Builder.Dag
        public class TestExample {
          public static class ScoreInput implements TaskInput {
            @ArgName("region_code") public String region;
            public double threshold;
            public List<String> tags;
          }

          @Builder.Task
          public double score(Client client, ScoreInput input) { return input.threshold; }

          @Wiring
          static void depends(TestExampleRef f) {
            f.score(In.value(new ScoreInput()));
          }
        }
      """,
      )

    assertThat(compilation).succeeded()
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleBuilder")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleBuilder",
        """
         package org.apache.airflow.example;

         import java.lang.Exception;
         import java.lang.Override;
         import java.lang.String;
         import java.util.List;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.internal.ArgValues;
         import org.apache.airflow.sdk.internal.Refs;

         public final class TestExampleBuilder {
           public static final String DAG_ID = "TestExample";

           public static DagDef dag() {
             var dag = new DagDef(DAG_ID);
             return dag;
           }

           public static DagDef build() {
             var dag = dag();
             TestExample.depends(new TestExampleRef(dag));
             Refs.requireRegistered(dag, List.of("score"));
             return dag;
           }

           public static final class Score implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               TestExample.ScoreInput input = ArgValues.bindInput(context, client, TestExample.ScoreInput.class);
               client.setXCom(new TestExample().score(client, input));
             }
           }
         }
        """,
      )
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleRef")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleRef",
        """
         package org.apache.airflow.example;

         import java.lang.Double;
         import java.util.List;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.In;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.TaskRef;
         import org.apache.airflow.sdk.internal.Refs;

         public final class TestExampleRef {
           private final DagDef dag;

           public TestExampleRef(DagDef dag) {
             this.dag = dag;
           }

           public TaskRef<Double> score(In<? extends TestExample.ScoreInput> input) {
             return Refs.register(dag, new TaskDef("score", TestExampleBuilder.Score.class), List.of(input));
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("bind a TaskArgs parameter without inspecting it as a bundle")
  fun generateBuilderBindsTaskArgsParam() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.TaskArgs;
        @Builder.Dag
        public class TestExample {
          @Builder.Task
          public void t(TaskArgs args) {}
        }
      """,
      )

    assertThat(compilation).succeeded()
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleBuilder")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleBuilder",
        """
         package org.apache.airflow.example;

         import java.lang.Exception;
         import java.lang.Override;
         import java.lang.String;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.TaskArgs;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.internal.ArgValues;

         public final class TestExampleBuilder {
           public static final String DAG_ID = "TestExample";

           public static DagDef dag() {
             var dag = new DagDef(DAG_ID);
             return dag;
           }

           public static DagDef build() {
             var dag = dag();
             dag.addTask(new TaskDef("t", T.class));
             return dag;
           }
           public static final class T implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               TaskArgs args = ArgValues.bindInput(context, client, TaskArgs.class);
               new TestExample().t(args);
             }
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("reject an input bundle mixed with flat data parameters")
  fun rejectBundleMixedWithFlatParams() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.TaskInput;
        @Builder.Dag
        public class TestExample {
          public static class ScoreInput implements TaskInput {
            public double threshold;
          }

          @Builder.Task
          public void t(ScoreInput input, int extra) {}
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Task method 't' declares TaskInput parameter 'input' and other data parameters",
    )
  }

  @Test
  @DisplayName("reject a task declaring more than one input bundle")
  fun rejectMultipleBundles() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.TaskInput;
        @Builder.Dag
        public class TestExample {
          public static class ScoreInput implements TaskInput {
            public double threshold;
          }

          @Builder.Task
          public void t(ScoreInput first, ScoreInput second) {}
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Task method 't' declares more than one TaskInput parameter: 'first', 'second'",
    )
  }

  @Test
  @DisplayName("reject an input bundle with a non-public field")
  fun rejectBundleWithNonPublicField() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.TaskInput;
        @Builder.Dag
        public class TestExample {
          public static class ScoreInput implements TaskInput {
            double threshold;
          }

          @Builder.Task
          public void t(ScoreInput input) {}
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "TaskInput field ScoreInput.threshold must be public and non-final",
    )
  }

  @Test
  @DisplayName("reject an input bundle without a public no-argument constructor")
  fun rejectBundleWithoutNoArgConstructor() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.TaskInput;
        @Builder.Dag
        public class TestExample {
          public static class ScoreInput implements TaskInput {
            public double threshold;

            public ScoreInput(double threshold) { this.threshold = threshold; }
          }

          @Builder.Task
          public void t(ScoreInput input) {}
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "TaskInput class ScoreInput needs a public no-argument constructor",
    )
  }

  @Test
  @DisplayName("generate builder for dag class with custom dag id")
  fun generateBuilderWithCustomDagId() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        @Builder.Dag(id = "foo") public class TestExample {}
      """,
      )
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleBuilder")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleBuilder",
        """
         package org.apache.airflow.example;
         import java.lang.String;
         import org.apache.airflow.sdk.DagDef;
         public final class TestExampleBuilder {
           public static final String DAG_ID = "foo";
           public static DagDef dag() { var dag = new DagDef(DAG_ID); return dag; }
           public static DagDef build() { var dag = dag(); return dag; }
         }
        """,
      )
  }

  @Test
  @DisplayName("generate builder for dag class with custom class name")
  fun generateBuilderWithCustomClassName() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        @Builder.Dag(to = "Foo") public class TestExample {}
      """,
      )
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.Foo")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.Foo",
        """
         package org.apache.airflow.example;
         import java.lang.String;
         import org.apache.airflow.sdk.DagDef;
         public final class Foo {
           public static final String DAG_ID = "TestExample";
           public static DagDef dag() { var dag = new DagDef(DAG_ID); return dag; }
           public static DagDef build() { var dag = dag(); return dag; }
         }
        """,
      )
  }

  @Test
  @DisplayName("generate builder for dag class with custom task name")
  fun generateBuilderForDagClassWithCustomTaskName() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.Wiring;
        @Builder.Dag
        public class TestExample {
          @Builder.Task(id = "foo") public void t1() {}

          @Wiring
          static void depends(TestExampleRef f) {
            f.t1();
          }
        }
      """,
      )

    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleBuilder")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleBuilder",
        """
         package org.apache.airflow.example;
         import java.lang.Exception;
         import java.lang.Override;
         import java.lang.String;
         import java.util.List;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.internal.Refs;
         public final class TestExampleBuilder {
           public static final String DAG_ID = "TestExample";
           public static DagDef dag() { var dag = new DagDef(DAG_ID); return dag; }
           public static DagDef build() {
             var dag = dag();
             TestExample.depends(new TestExampleRef(dag));
             Refs.requireRegistered(dag, List.of("foo"));
             return dag;
           }
           public static final class T1 implements Task {
             @Override public void execute(Context context, Client client) throws Exception { new TestExample().t1(); }
           }
         }
        """,
      )
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleRef")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleRef",
        """
         package org.apache.airflow.example;
         import java.lang.Void;
         import java.util.List;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.TaskRef;
         import org.apache.airflow.sdk.internal.Refs;
         public final class TestExampleRef {
           private final DagDef dag;
           public TestExampleRef(DagDef dag) { this.dag = dag; }
           public TaskRef<Void> t1() {
             return Refs.register(dag, new TaskDef("foo", TestExampleBuilder.T1.class), List.of());
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("reject wiring that feeds an incompatible upstream type")
  fun rejectIncompatibleWiring() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.Wiring;
        @Builder.Dag
        public class TestExample {
          @Builder.Task
          public String ps() { return "x"; }

          @Builder.Task
          public void t(int v) {}

          @Wiring
          static void depends(TestExampleRef f) {
            f.t(f.ps());
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining("incompatible types")
  }

  @Test
  @DisplayName("reject more than one wiring method")
  fun rejectMultipleWiringMethods() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.Wiring;
        @Builder.Dag
        public class TestExample {
          @Builder.Task public void t1() {}

          @Wiring
          static void one(TestExampleRef f) { f.t1(); }

          @Wiring
          static void two(TestExampleRef f) {}
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Dag class TestExample declares more than one @Wiring method: one, two",
    )
  }

  @Test
  @DisplayName("reject a non-static wiring method")
  fun rejectNonStaticWiring() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.Wiring;
        @Builder.Dag
        public class TestExample {
          @Builder.Task public void t1() {}

          @Wiring
          void depends(TestExampleRef f) { f.t1(); }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Wiring method 'depends' must be static and non-private",
    )
  }

  @Test
  @DisplayName("reject a wiring method with the wrong shape")
  fun rejectWrongShapedWiring() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.Wiring;
        @Builder.Dag
        public class TestExample {
          @Builder.Task public void t1() {}

          @Wiring
          static void depends(TestExampleRef f, int extra) { f.t1(); }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Wiring method 'depends' must be void and take the generated TestExampleRef as its only parameter",
    )
  }

  @Test
  @DisplayName("generate builder for dag class with varargs task parameter")
  fun generateBuilderForDagClassWithVarArgsTaskParameter() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        @Builder.Dag
        public class TestExample { @Builder.Task(id = "foo") public void t1(String... client) {} }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Cannot create task from vararg function t1",
    )
  }

  @Test
  @DisplayName("reject duplicate task ids")
  fun rejectDuplicateTaskIds() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        @Builder.Dag
        public class TestExample {
          @Builder.Task(id = "x")
          public void t1() {}

          @Builder.Task(id = "x")
          public void t2() {}
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining("Tasks in Dag have duplicate ID: x")
  }

  @Test
  @DisplayName("reject a duration attribute that is not ISO-8601")
  fun rejectInvalidDurationAttribute() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        @Builder.Dag
        public class TestExample {
          @Builder.Task(retryDelay = "5 minutes") public void t() {}
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Annotation attribute 'retryDelay' is not valid ISO-8601: '5 minutes'",
    )
  }
}
