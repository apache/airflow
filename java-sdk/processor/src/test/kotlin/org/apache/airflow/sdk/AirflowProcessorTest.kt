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
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.nio.file.Files
import javax.tools.JavaFileObject

private fun compile(source: String) =
  Compiler.javac().withProcessors(AirflowProcessor()).compile(
    JavaFileObjects.forSourceString("org.apache.airflow.example.TestExample", source),
  )

private fun JavaFileObjectSubject.hasSourceEquivalentTo(
  qual: String,
  source: String,
) = hasSourceEquivalentTo(
  JavaFileObjects.forSourceString(qual, source),
)

class AirflowProcessorTest {
  @Test
  @DisplayName("generate builder and task-reference twins for dag class")
  fun generateBuilderAndRefForDagClass() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;

        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.Client;
        import org.apache.airflow.sdk.Context;

        @Airflow.Dag
        public class TestExample {
          @Airflow.Task
          public void t1() {}

          @Airflow.Task
          public int t2(Client client) {
            return 7;
          }

          @Airflow.Task
          public void t3(Context ctx, int value) {
            System.out.println(String.format("%s %s", ctx.ti, value));
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {
              t1();
              t3(t2());
            }
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
         import java.util.List;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.internal.DagSource;
         import org.apache.airflow.sdk.internal.Refs;
         import org.apache.airflow.sdk.internal.TaskArgs;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = DagSource.declaredBy(new DagDef("TestExample"), TestExample.class);
             return Refs.record(dag, List.of("t1", "t2", "t3"), List.of(), new TestExample.Wiring()::depends);
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
               TaskArgs args = TaskArgs.of(context, client, 1);
               int value = args.require(0, Integer.class);
               new TestExample().t3(context, value);
             }
           }
         }
        """,
      )
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleDeps")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleDeps",
        """
         package org.apache.airflow.example;

         import java.lang.Integer;
         import java.lang.Void;
         import java.util.List;
         import org.apache.airflow.sdk.Arg;
         import org.apache.airflow.sdk.Deps;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.TaskRef;
         import org.apache.airflow.sdk.internal.Refs;

         /**
          * Wiring view of {@link TestExample}'s task methods, for declaring its task graph.
          *
          * <p>Calling one registers its task with the Dag being built; passing the handle it
          * returned into another call feeds the upstream's output into that task's parameter
          * and wires the data edge. {@code before} and {@code after} wire an ordering-only edge.
          */
         public interface TestExampleDeps extends Deps {
           default TaskRef<Void> t1() {
             return Refs.node("", new TaskDef("t1", TestExampleBuilder.T1.class));
           }

           default TaskRef<Integer> t2() {
             return Refs.node("", new TaskDef("t2", TestExampleBuilder.T2.class));
           }

           default TaskRef<Void> t3(Arg<? extends Integer> value) {
             return Refs.call("", new TaskDef("t3", TestExampleBuilder.T3.class), List.of("value"), value);
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
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.Client;
        import org.apache.airflow.sdk.Context;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task
          public void t(long first, Client client, String second, Context ctx, Integer third) {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() { t(lit(1L), lit("second"), lit(3)); }
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
         import java.lang.Long;
         import java.lang.Override;
         import java.lang.String;
         import java.util.List;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.internal.DagSource;
         import org.apache.airflow.sdk.internal.Refs;
         import org.apache.airflow.sdk.internal.TaskArgs;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = DagSource.declaredBy(new DagDef("TestExample"), TestExample.class);
             return Refs.record(dag, List.of("t"), List.of(), new TestExample.Wiring()::depends);
           }

           public static final class T implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               TaskArgs args = TaskArgs.of(context, client, 3);
               long first = args.require(0, Long.class);
               String second = args.get(1, String.class);
               Integer third = args.get(2, Integer.class);
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
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task
          public void t(boolean flag, float fraction, Double boxed, List<String> tags, Map raw) {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() { t(lit(true), lit(1f), lit(2.0), lit(List.of("a")), lit(Map.of())); }
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
         import org.apache.airflow.sdk.internal.DagSource;
         import org.apache.airflow.sdk.internal.Refs;
         import org.apache.airflow.sdk.internal.TaskArgs;
         import org.apache.airflow.sdk.internal.TypeRef;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = DagSource.declaredBy(new DagDef("TestExample"), TestExample.class);
             return Refs.record(dag, List.of("t"), List.of(), new TestExample.Wiring()::depends);
           }

           public static final class T implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               TaskArgs args = TaskArgs.of(context, client, 5);
               boolean flag = args.require(0, Boolean.class);
               float fraction = args.require(1, Float.class);
               Double boxed = args.get(2, Double.class);
               List<String> tags = args.get(3, new TypeRef<List<String>>() {});
               Map raw = args.get(4, Map.class);
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
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task
          public String ps() { return "x"; }

          @Airflow.Task
          public void pv() {}

          @Airflow.Task
          public List<String> pl() { return null; }

          @Airflow.Task
          public long pn() { return 1L; }

          @Airflow.Task
          public void t(String text, Object anything, List<String> items, Long boxed) {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {
              t(ps(), pv(), pl(), pn());
            }
          }
        }
      """,
      )

    assertThat(compilation).succeeded()
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleDeps")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleDeps",
        """
         package org.apache.airflow.example;

         import java.lang.Long;
         import java.lang.String;
         import java.lang.Void;
         import java.util.List;
         import org.apache.airflow.sdk.Arg;
         import org.apache.airflow.sdk.Deps;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.TaskRef;
         import org.apache.airflow.sdk.internal.Refs;

         /**
          * Wiring view of {@link TestExample}'s task methods, for declaring its task graph.
          *
          * <p>Calling one registers its task with the Dag being built; passing the handle it
          * returned into another call feeds the upstream's output into that task's parameter
          * and wires the data edge. {@code before} and {@code after} wire an ordering-only edge.
          */
         public interface TestExampleDeps extends Deps {
           default TaskRef<String> ps() {
             return Refs.node("", new TaskDef("ps", TestExampleBuilder.Ps.class));
           }

           default TaskRef<Void> pv() {
             return Refs.node("", new TaskDef("pv", TestExampleBuilder.Pv.class));
           }

           default TaskRef<List<String>> pl() {
             return Refs.node("", new TaskDef("pl", TestExampleBuilder.Pl.class));
           }

           default TaskRef<Long> pn() {
             return Refs.node("", new TaskDef("pn", TestExampleBuilder.Pn.class));
           }

           default TaskRef<Void> t(Arg<? extends String> text, Arg<?> anything,
               Arg<? extends List<String>> items, Arg<? extends Long> boxed) {
             return Refs.call("", new TaskDef("t", TestExampleBuilder.T.class), List.of("text", "anything", "items", "boxed"), text, anything, items, boxed);
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
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag(id = "cfg", schedule = "@daily", queue = "java", tags = {"a", "b"}, catchup = true,
            startDate = "2026-01-01T00:00:00Z")
        public class TestExample {
          @Airflow.Task(retries = 2, queue = "q", retryDelay = "PT5M", retryExponentialBackoff = 1.5)
          public void t1() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {
              t1();
            }
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
         import java.time.OffsetDateTime;
         import java.util.List;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.internal.DagSource;
         import org.apache.airflow.sdk.internal.Refs;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = DagSource.declaredBy(new DagDef("cfg"), TestExample.class);
             dag.config("schedule", "@daily");
             dag.config("queue", "java");
             dag.config("tags", List.of("a", "b"));
             dag.config("catchup", true);
             dag.config("start_date", OffsetDateTime.parse("2026-01-01T00:00:00Z"));
             return Refs.record(dag, List.of("t1"), List.of(), new TestExample.Wiring()::depends);
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
      .generatedSourceFile("org.apache.airflow.example.TestExampleDeps")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleDeps",
        """
         package org.apache.airflow.example;

         import java.lang.Void;
         import java.time.Duration;
         import org.apache.airflow.sdk.Deps;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.TaskRef;
         import org.apache.airflow.sdk.internal.Refs;

         /**
          * Wiring view of {@link TestExample}'s task methods, for declaring its task graph.
          *
          * <p>Calling one registers its task with the Dag being built; passing the handle it
          * returned into another call feeds the upstream's output into that task's parameter
          * and wires the data edge. {@code before} and {@code after} wire an ordering-only edge.
          */
         public interface TestExampleDeps extends Deps {
           default TaskRef<Void> t1() {
             return Refs.node("", new TaskDef("t1", TestExampleBuilder.T1.class).config("retries", 2).config("queue", "q").config("retry_delay", Duration.parse("PT5M")).config("retry_exponential_backoff", 1.5));
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("keep the positional handle from clashing with a parameter named args")
  fun generateBuilderAvoidsClashWithParamNamedArgs() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task
          public void t(String args, int other) {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() { t(lit("a"), lit(1)); }
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
         import org.apache.airflow.sdk.internal.DagSource;
         import org.apache.airflow.sdk.internal.Refs;
         import org.apache.airflow.sdk.internal.TaskArgs;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = DagSource.declaredBy(new DagDef("TestExample"), TestExample.class);
             return Refs.record(dag, List.of("t"), List.of(), new TestExample.Wiring()::depends);
           }
           public static final class T implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               TaskArgs args_ = TaskArgs.of(context, client, 2);
               String args = args_.get(0, String.class);
               int other = args_.require(1, Integer.class);
               new TestExample().t(args, other);
             }
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("keep generated locals from clashing with the injected client and context")
  fun generateBuilderAvoidsClashWithInjectedParamNames() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import java.util.List;
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.TaskInput;
        @Airflow.Dag
        public class TestExample {
          public static class ScoreInput implements TaskInput {
            public double threshold;
          }

          @Airflow.Task
          public void flat(String client, int context) {}

          @Airflow.Task
          public void named(ScoreInput context) {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() { flat(lit("a"), lit(1)); named(lit(null)); }
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
         import org.apache.airflow.sdk.internal.DagSource;
         import org.apache.airflow.sdk.internal.Refs;
         import org.apache.airflow.sdk.internal.TaskArgs;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = DagSource.declaredBy(new DagDef("TestExample"), TestExample.class);
             return Refs.record(dag, List.of("flat", "named"), List.of(), new TestExample.Wiring()::depends);
           }

           public static final class Flat implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               TaskArgs args = TaskArgs.of(context, client, 2);
               String client_ = args.get(0, String.class);
               int context_ = args.require(1, Integer.class);
               new TestExample().flat(client_, context_);
             }
           }

           public static final class Named implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               TestExample.ScoreInput context_ = ArgValues.bindInput(context, client, TestExample.ScoreInput.class);
               new TestExample().named(context_);
             }
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("reject a TaskInput whose inherited field cannot be assigned")
  fun rejectTaskInputWithNonPublicInheritedField() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.TaskInput;
        @Airflow.Dag
        public class TestExample {
          public static class BaseInput {
            private String secret;
          }

          public static class ScoreInput extends BaseInput implements TaskInput {
            public double threshold;
          }

          @Airflow.Task
          public void t(ScoreInput input) {}
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "TaskInput field ScoreInput.secret must be public and non-final",
    )
  }

  @Test
  @DisplayName("reject a TaskInput whose inherited field folds onto a declared one")
  fun rejectTaskInputWithCollidingInheritedField() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.TaskInput;
        @Airflow.Dag
        public class TestExample {
          public static class BaseInput {
            public String regionCode;
          }

          public static class ScoreInput extends BaseInput implements TaskInput {
            public String region_code;
          }

          @Airflow.Task
          public void t(ScoreInput input) {}
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "TaskInput fields ScoreInput.region_code and ScoreInput.regionCode claim argument names that " +
        "differ only in case or underscores",
    )
  }

  @Test
  @DisplayName("reject a TaskInput mixed with flat data parameters")
  fun rejectTaskInputMixedWithFlatParams() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.TaskInput;
        @Airflow.Dag
        public class TestExample {
          public static class ScoreInput implements TaskInput {
            public double threshold;
          }

          @Airflow.Task
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
  @DisplayName("reject a task declaring more than one TaskInput")
  fun rejectMultipleTaskInputs() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.TaskInput;
        @Airflow.Dag
        public class TestExample {
          public static class ScoreInput implements TaskInput {
            public double threshold;
          }

          @Airflow.Task
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
  @DisplayName("reject a TaskInput with a non-public field")
  fun rejectTaskInputWithNonPublicField() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.TaskInput;
        @Airflow.Dag
        public class TestExample {
          public static class ScoreInput implements TaskInput {
            double threshold;
          }

          @Airflow.Task
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
  @DisplayName("reject a TaskInput without a public no-argument constructor")
  fun rejectTaskInputWithoutNoArgConstructor() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.TaskInput;
        @Airflow.Dag
        public class TestExample {
          public static class ScoreInput implements TaskInput {
            public double threshold;

            public ScoreInput(double threshold) { this.threshold = threshold; }
          }

          @Airflow.Task
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
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag(id = "foo")
        public class TestExample {
          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
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
         import java.util.List;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.internal.DagSource;
         import org.apache.airflow.sdk.internal.Refs;
         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = DagSource.declaredBy(new DagDef("foo"), TestExample.class);
             return Refs.record(dag, List.of(), List.of(), new TestExample.Wiring()::depends);
           }
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
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag(to = "Foo")
        public class TestExample {
          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.Foo")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.Foo",
        """
         package org.apache.airflow.example;
         import java.util.List;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.internal.DagSource;
         import org.apache.airflow.sdk.internal.Refs;
         public final class Foo {
           public static DagDef build() {
             var dag = DagSource.declaredBy(new DagDef("TestExample"), TestExample.class);
             return Refs.record(dag, List.of(), List.of(), new TestExample.Wiring()::depends);
           }
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
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task(id = "foo") public void t1() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {
              t1();
            }
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
         import java.util.List;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.internal.DagSource;
         import org.apache.airflow.sdk.internal.Refs;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = DagSource.declaredBy(new DagDef("TestExample"), TestExample.class);
             return Refs.record(dag, List.of("foo"), List.of(), new TestExample.Wiring()::depends);
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
      .generatedSourceFile("org.apache.airflow.example.TestExampleDeps")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleDeps",
        """
         package org.apache.airflow.example;

         import java.lang.Void;
         import org.apache.airflow.sdk.Deps;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.TaskRef;
         import org.apache.airflow.sdk.internal.Refs;

         /**
          * Wiring view of {@link TestExample}'s task methods, for declaring its task graph.
          *
          * <p>Calling one registers its task with the Dag being built; passing the handle it
          * returned into another call feeds the upstream's output into that task's parameter
          * and wires the data edge. {@code before} and {@code after} wire an ordering-only edge.
          */
         public interface TestExampleDeps extends Deps {
           default TaskRef<Void> t1() {
             return Refs.node("", new TaskDef("foo", TestExampleBuilder.T1.class));
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
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task
          public String ps() { return "x"; }

          @Airflow.Task
          public void t(int v) {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() { t(ps()); }
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining("incompatible types")
  }

  @Test
  @DisplayName("reject wiring a numeric upstream into a narrower numeric parameter")
  fun rejectLossyNumericWiring() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task
          public double ratio() { return 2.7; }

          @Airflow.Task
          public void load(long rows) {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() { load(ratio()); }
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining("incompatible types")
  }

  @Test
  @DisplayName("reject a dag class with no wiring class")
  fun rejectDagClassWithoutWiringClass() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void t1() {}
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Dag class TestExample must declare an @Airflow.Deps class implementing TestExampleDeps " +
        "to declare its task graph; a class of task bodies for a Dag the Python file owns carries " +
        "@Airflow.TaskHandler instead",
    )
  }

  @Test
  @DisplayName("reject more than one wiring class")
  fun rejectMultipleWiringClasses() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void t1() {}

          @Airflow.Deps
          static class One implements TestExampleDeps {
            void depends() { t1(); }
          }

          @Airflow.Deps
          static class Two implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Dag class TestExample declares more than one @Airflow.Deps class: One, Two",
    )
  }

  @Test
  @DisplayName("reject a non-static wiring class")
  fun rejectNonStaticWiringClass() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void t1() {}

          @Airflow.Deps
          class Wiring implements TestExampleDeps {
            void depends() { t1(); }
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Airflow.Deps class 'Wiring' must be static and non-private",
    )
  }

  @Test
  @DisplayName("reject a wiring class with no depends() method")
  fun rejectWiringClassWithoutDepends() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void t1() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void wireItUp() { t1(); }
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Airflow.Deps class 'Wiring' must have a non-private, no-argument depends() method",
    )
  }

  @Test
  @DisplayName("reject a wiring class that implements another Dag's wiring view")
  fun rejectWiringClassOfAnotherDag() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void t1() {}

          @Airflow.Deps
          static class Wiring implements OtherDeps {
            void depends() { t1(); }
          }
        }

        @Airflow.Dag
        class Other {
          @Airflow.Task public void t1() {}

          @Airflow.Deps
          static class Wiring implements OtherDeps {
            void depends() { t1(); }
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Airflow.Deps class 'Wiring' must implement TestExampleDeps, the wiring view of TestExample",
    )
  }

  @Test
  @DisplayName("reject an abstract wiring class")
  fun rejectAbstractWiringClass() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void t1() {}

          @Airflow.Deps
          abstract static class Wiring implements TestExampleDeps {
            void depends() { t1(); }
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining("@Airflow.Deps 'Wiring' must be a concrete class")
  }

  @Test
  @DisplayName("reject a wiring class with no no-argument constructor")
  fun rejectWiringClassWithoutNoArgConstructor() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void t1() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            Wiring(int unused) {}
            void depends() { t1(); }
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Airflow.Deps class 'Wiring' needs a non-private no-argument constructor",
    )
  }

  @Test
  @DisplayName("reject a depends() that throws a checked exception")
  fun rejectDependsThrowingCheckedException() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void t1() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() throws java.io.IOException { t1(); }
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "depends() of @Airflow.Deps class 'Wiring' must not throw checked exceptions: java.io.IOException",
    )
  }

  @Test
  @DisplayName("accept a depends() the wiring class inherits")
  fun acceptInheritedDepends() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void t1() {}

          static class Base implements TestExampleDeps {
            void depends() { t1(); }
          }

          @Airflow.Deps
          static class Wiring extends Base implements TestExampleDeps {}
        }
      """,
      )
    assertThat(compilation).succeeded()
  }

  @Test
  @DisplayName("reject a wiring class outside a Dag class")
  fun rejectMisplacedWiringClass() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        public class TestExample {
          @Airflow.Deps
          static class Wiring {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Airflow.Deps class 'Wiring' must be nested directly in an @Airflow.Dag class",
    )
  }

  @Test
  @DisplayName("reject a task method whose name clashes with a wiring-view member")
  fun rejectTaskNameClashingWithViewMember() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task(id = "wire") public void depends() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Task method 'depends' clashes with a member of the wiring view; rename the method and keep " +
        "the task id with @Airflow.Task(id = \"wire\")",
    )
  }

  @Test
  @DisplayName("nest a wiring view per task group, keyed by the class tree")
  fun generateBuilderWithTaskGroups() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void extract() {}

          @Airflow.TaskGroup
          static class Staging {
            @Airflow.Task public void stage() {}

            @Airflow.TaskGroup(id = "checks")
            static class Checks {
              @Airflow.Task public void nulls() {}
            }
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {
              extract().before(staging());
              staging().stage().before(staging().checks().nulls());
            }
          }
        }
      """,
      )

    assertThat(compilation).succeeded()
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleBuilder")
      .contentsAsUtf8String()
      .contains(
        "return Refs.record(dag, List.of(\"extract\", \"Staging.stage\", \"Staging.checks.nulls\"), " +
          "List.of(\"Staging\", \"Staging.checks\"), new TestExample.Wiring()::depends);",
      )
    val view = assertThat(compilation).generatedSourceFile("org.apache.airflow.example.TestExampleDeps")
    view.contentsAsUtf8String().contains("default Staging staging() {")
    view.contentsAsUtf8String().contains("interface Staging extends Deps.TaskGroup {")
    view.contentsAsUtf8String().contains("return \"Staging.checks\";")
    view.contentsAsUtf8String().contains(
      "return Refs.node(groupId(), new TaskDef(\"Staging.checks.nulls\", " +
        "TestExampleBuilder.Staging_Checks_Nulls.class));",
    )
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleBuilder")
      .contentsAsUtf8String()
      .contains("public static final class Staging_Checks_Nulls implements Task {")
  }

  @Test
  @DisplayName("scope task method names to their own task group")
  fun generateBuilderScopesTaskNamesPerGroup() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.TaskGroup
          static class First {
            @Airflow.Task public void run() {}
          }

          @Airflow.TaskGroup
          static class Second {
            @Airflow.Task public void run() {}
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() { first().run().before(second().run()); }
          }
        }
      """,
      )

    assertThat(compilation).succeeded()
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleBuilder")
      .contentsAsUtf8String()
      .contains("public static final class First_Run implements Task {")
  }

  @Test
  @DisplayName("let a wiring class label an edge with Label.of(...).on(...)")
  fun compileWiringThatLabelsAnEdge() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.Label;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void extract() {}
          @Airflow.Task public void load() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() { extract().before(Label.of("rows").on(load())); }
          }
        }
      """,
      )

    assertThat(compilation).succeeded()
  }

  @Test
  @DisplayName("fail to compile a label passed to before without on")
  fun failOnLabelWithoutOn() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.Label;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void extract() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() { extract().before(Label.of("rows")); }
          }
        }
      """,
      )

    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "org.apache.airflow.sdk.Label cannot be converted to org.apache.airflow.sdk.Deps.Flow",
    )
  }

  @Test
  @DisplayName("fail to compile Label.on called with no task")
  fun failOnLabelOnWithoutTask() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.Label;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void extract() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() { extract().before(Label.of("rows").on()); }
          }
        }
      """,
      )

    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "method on in class org.apache.airflow.sdk.Label cannot be applied to given types",
    )
  }

  @Test
  @DisplayName("reject a task group ID that is not a plain identifier")
  fun rejectInvalidTaskGroupId() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.TaskGroup(id = "staging.checks")
          static class Staging {
            @Airflow.Task public void t1() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Task group ID 'staging.checks' must contain only ASCII letters, digits, underscores, or dashes",
    )
  }

  @Test
  @DisplayName("reject a non-static task group class")
  fun rejectNonStaticTaskGroupClass() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.TaskGroup
          class Staging {
            @Airflow.Task public void t1() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Airflow.TaskGroup class 'Staging' must be static and non-private",
    )
  }

  @Test
  @DisplayName("reject a task group whose accessor clashes with a task method")
  fun rejectTaskGroupClashingWithTaskMethod() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void staging() {}

          @Airflow.TaskGroup
          static class Staging {
            @Airflow.Task public void t1() {}
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Task group class 'Staging' and task method 'staging' would both be 'staging()' on the wiring " +
        "view; rename one",
    )
  }

  @Test
  @DisplayName("reject an abstract task group class")
  fun rejectAbstractTaskGroupClass() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.TaskGroup
          abstract static class Staging {
            @Airflow.Task public void t1() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining("@Airflow.TaskGroup 'Staging' must be a concrete class")
  }

  @Test
  @DisplayName("reject a task group class with no no-argument constructor")
  fun rejectTaskGroupClassWithoutNoArgConstructor() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.TaskGroup
          static class Staging {
            Staging(String name) {}

            @Airflow.Task public void t1() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Airflow.TaskGroup class 'Staging' needs a non-private no-argument constructor",
    )
  }

  @Test
  @DisplayName("reject a task in a group whose name clashes with the group view")
  fun rejectTaskClashingWithGroupView() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.TaskGroup
          static class Staging {
            @Airflow.Task public void nodes() {}
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Task method 'nodes' clashes with a member of the wiring view; rename the method and keep the " +
        "task id with @Airflow.Task(id = \"nodes\")",
    )
  }

  @Test
  @DisplayName("reject a task in a group named after a member the view inherits from Flow")
  fun rejectTaskNamedBeforeInGroup() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.TaskGroup
          static class Staging {
            @Airflow.Task public void before() {}
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Task method 'before' clashes with a member of the wiring view; rename the method and keep the " +
        "task id with @Airflow.Task(id = \"before\")",
    )
  }

  @Test
  @DisplayName("reject two task groups in one scope with the same id")
  fun rejectDuplicateTaskGroupIds() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.TaskGroup(id = "checks")
          static class Alpha {
            @Airflow.Task public void t1() {}
          }

          @Airflow.TaskGroup(id = "checks")
          static class Beta {
            @Airflow.Task public void t2() {}
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining("Class TestExample declares more than one task group 'checks'")
  }

  @Test
  @DisplayName("accept a task named after a group view member outside a group")
  fun acceptTaskNamedAfterGroupViewMemberAtTopLevel() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task public void nodes() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            public void depends() { nodes(); }
          }
        }
      """,
      )
    assertThat(compilation).succeeded()
  }

  @Test
  @DisplayName("reject two task group classes that share an accessor")
  fun rejectTaskGroupsSharingAnAccessor() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.TaskGroup
          static class Staging {
            @Airflow.Task public void t1() {}
          }

          @Airflow.TaskGroup(id = "lower")
          static class staging {
            @Airflow.Task public void t2() {}
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Task group classes 'Staging' and 'staging' would both be 'staging()' on the wiring view; rename one",
    )
  }

  @Test
  @DisplayName("accept a task id that carries a dot, as Python's KEY_REGEX does")
  fun acceptDottedTaskId() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task(id = "staging.stage") public void stage() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            public void depends() { stage(); }
          }
        }
      """,
      )
    assertThat(compilation).succeeded()
  }

  @Test
  @DisplayName("reject a task and a task group sharing an id")
  fun rejectTaskSharingIdWithGroup() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task(id = "Staging") public void staged() {}

          @Airflow.TaskGroup
          static class Staging {
            @Airflow.Task public void t1() {}
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Dag has both a task and a task group with ID 'Staging'; rename one",
    )
  }

  @Test
  @DisplayName("reject a task in a task group that takes the id of one of the group's join nodes")
  fun rejectTaskTakingJoinNodeIdOfItsGroup() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.TaskGroup
          static class Transform {
            @Airflow.Task(id = "upstream_join_id") public void clean() {}
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Task 'Transform.upstream_join_id' has the ID of a join node of task group 'Transform'; rename the task",
    )
  }

  @Test
  @DisplayName("reject a top-level task that takes the id of a task group's join node")
  fun rejectTopLevelTaskTakingJoinNodeId() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task(id = "Transform.downstream_join_id") public void publish() {}

          @Airflow.TaskGroup
          static class Transform {
            @Airflow.Task public void clean() {}
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Task 'Transform.downstream_join_id' has the ID of a join node of task group 'Transform'; rename the task",
    )
  }

  @Test
  @DisplayName("reject a task in a nested task group that takes the id of one of that group's join nodes")
  fun rejectTaskTakingJoinNodeIdOfNestedGroup() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.TaskGroup
          static class Transform {
            @Airflow.TaskGroup
            static class Checks {
              @Airflow.Task(id = "downstream_join_id") public void nulls() {}
            }
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Task 'Transform.Checks.downstream_join_id' has the ID of a join node of task group 'Transform.Checks'; " +
        "rename the task",
    )
  }

  @Test
  @DisplayName("reject a nested task group that takes the id of a join node of the group holding it")
  fun rejectNestedTaskGroupTakingJoinNodeId() {
    listOf("upstream_join_id", "downstream_join_id").forEach { id ->
      val compilation =
        compile(
          """
          package org.apache.airflow.example;
          import org.apache.airflow.sdk.Airflow;
          @Airflow.Dag
          public class TestExample {
            @Airflow.TaskGroup
            static class Transform {
              @Airflow.TaskGroup(id = "$id")
              static class Inner {
                @Airflow.Task public void clean() {}
              }
            }

            @Airflow.Deps
            static class Wiring implements TestExampleDeps {
              void depends() {}
            }
          }
        """,
        )
      assertThat(compilation).failed()
      assertThat(compilation).hadErrorContaining(
        "Task group 'Transform.$id' has the ID of a join node of task group 'Transform'; rename the nested group",
      )
    }
  }

  @Test
  @DisplayName("reject two task methods whose generated classes would collide")
  fun rejectCollidingGeneratedTaskClasses() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.TaskGroup(id = "flat")
          static class A_B {
            @Airflow.Task public void c() {}
          }

          @Airflow.TaskGroup(id = "outer")
          static class A {
            @Airflow.TaskGroup(id = "inner")
            static class B {
              @Airflow.Task public void c() {}
            }
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "both generate the task class 'A_B_C'; rename one of them or an enclosing @Airflow.TaskGroup class",
    )
  }

  @Test
  @DisplayName("reject a task group class that is not nested in a dag or another group")
  fun rejectMisplacedTaskGroup() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        public class TestExample {
          @Airflow.TaskGroup
          static class Staging {
            @Airflow.Task public void t1() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Airflow.TaskGroup class 'Staging' must be nested in an @Airflow.Dag class or in another " +
        "@Airflow.TaskGroup class",
    )
  }

  @Test
  @DisplayName("generate builder for dag class with varargs task parameter")
  fun generateBuilderForDagClassWithVarArgsTaskParameter() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample { @Airflow.Task(id = "foo") public void t1(String... client) {} }
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
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task(id = "x")
          public void t1() {}

          @Airflow.Task(id = "x")
          public void t2() {}
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining("Tasks in Dag have duplicate ID: x")
  }

  @Test
  @DisplayName("reject overloaded task methods, whatever their parameters")
  fun rejectOverloadedTaskMethods() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task(id = "a") public void extract() {}
          @Airflow.Task(id = "b") public void extract(String text) {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Class TestExample overloads task method 'extract'; a method's name is the name of its " +
        "generated task class and of its wiring-view method, so rename one and keep its task id " +
        "with @Airflow.Task(id = \"b\")",
    )
  }

  @Test
  @DisplayName("reject overloaded task-handler methods")
  fun rejectOverloadedTaskHandlerMethods() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        public class TestExample {
          @Airflow.TaskHandler(dag = "etl", task = "a") public void score() {}
          @Airflow.TaskHandler(dag = "etl", task = "b") public void score(String text) {}
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Class TestExample overloads task-handler method 'score'; a method's name is the name of its " +
        "generated task class, so rename one and keep its task id with " +
        "@Airflow.TaskHandler(task = \"b\")",
    )
  }

  @Test
  @DisplayName("reject a duration attribute that is not ISO-8601")
  fun rejectInvalidDurationAttribute() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task(retryDelay = "5 minutes") public void t() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() { t(); }
          }
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Annotation attribute 'retryDelay' is not valid ISO-8601: '5 minutes'",
    )
  }

  @Test
  @DisplayName("reject a blank Dag-level queue")
  fun rejectBlankDagQueue() {
    for (queue in listOf("", " ")) {
      val compilation =
        compile(
          """
          package org.apache.airflow.example;
          import org.apache.airflow.sdk.Airflow;
          @Airflow.Dag(queue = "$queue")
          public class TestExample {
            @Airflow.Task public void t() {}

            @Airflow.Deps
            static class Wiring implements TestExampleDeps {
              void depends() { t(); }
            }
          }
        """,
        )
      assertThat(compilation).failed()
      assertThat(compilation).hadErrorContaining("Annotation attribute 'queue' must not be blank")
    }
  }

  @Test
  @DisplayName("reject a blank task-level queue")
  fun rejectBlankTaskQueue() {
    for (queue in listOf("", " ")) {
      val compilation =
        compile(
          """
          package org.apache.airflow.example;
          import org.apache.airflow.sdk.Airflow;
          @Airflow.Dag
          public class TestExample {
            @Airflow.Task(queue = "$queue") public void t() {}

            @Airflow.Deps
            static class Wiring implements TestExampleDeps {
              void depends() { t(); }
            }
          }
        """,
        )
      assertThat(compilation).failed()
      assertThat(compilation).hadErrorContaining("Annotation attribute 'queue' must not be blank")
    }
  }

  @Test
  @DisplayName("escape quotes and backslashes in a string-array attribute")
  fun generateBuilderEscapesStringArrayValues() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag(tags = {"say \"hi\"", "back\\slash"})
        public class TestExample {
          @Airflow.Task public void t() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() { t(); }
          }
        }
      """,
      )

    assertThat(compilation).succeeded()
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleBuilder")
      .contentsAsUtf8String()
      .contains("""dag.config("tags", List.of("say \"hi\"", "back\\slash"));""")
  }

  @Test
  @DisplayName("escape quotes and backslashes in the task ids the wiring must register")
  fun generateBuilderEscapesWiredTaskIds() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task(id = "say \"hi\"") public void a() {}
          @Airflow.Task(id = "back\\slash") public void b() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {
              a();
              b();
            }
          }
        }
      """,
      )

    assertThat(compilation).succeeded()
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleBuilder")
      .contentsAsUtf8String()
      .contains("""List.of("say \"hi\"", "back\\slash")""")
  }

  @Test
  @DisplayName("bind a TaskInput through the shared populator")
  fun generateBuilderBindsTaskInputFields() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import java.util.List;
        import org.apache.airflow.sdk.ArgName;
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.Client;
        import org.apache.airflow.sdk.TaskInput;
        @Airflow.Dag
        public class TestExample {
          public static class ScoreInput implements TaskInput {
            @ArgName("region_code") public String region;
            public double threshold;
            public List<String> tags;
          }

          @Airflow.Task
          public double score(Client client, ScoreInput input) { return input.threshold; }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() { score(lit(null)); }
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
         import java.util.List;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.internal.ArgValues;
         import org.apache.airflow.sdk.internal.DagSource;
         import org.apache.airflow.sdk.internal.Refs;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = DagSource.declaredBy(new DagDef("TestExample"), TestExample.class);
             return Refs.record(dag, List.of("score"), List.of(), new TestExample.Wiring()::depends);
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
  }

  @Test
  @DisplayName("generate a registrar binding each handler to the ids its annotation names")
  fun generateHandlerRegistrar() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.Client;
        public class TestExample {
          @Airflow.TaskHandler(dag = "etl", task = "score")
          public long score(Client client, long rows) { return rows; }

          @Airflow.TaskHandler(dag = "etl")
          public void audit() {}
        }
      """,
      )

    assertThat(compilation).succeeded()
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleHandlers")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleHandlers",
        """
         package org.apache.airflow.example;

         import java.lang.Exception;
         import java.lang.Long;
         import java.lang.Override;
         import org.apache.airflow.sdk.Bundle;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.internal.TaskArgs;

         /**
          * Registers {@link TestExample}'s task handlers against the Dags the Python file owns.
          */
         public final class TestExampleHandlers {
           public static void registerInto(Bundle bundle) {
             bundle.register("etl", "score", Score.class);
             bundle.register("etl", "audit", Audit.class);
           }

           public static final class Score implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               TaskArgs args = TaskArgs.of(context, client, 1);
               long rows = args.require(0, Long.class);
               client.setXCom(new TestExample().score(client, rows));
             }
           }

           public static final class Audit implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               new TestExample().audit();
             }
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("reject a handler that names no Dag")
  fun rejectHandlerWithoutDag() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        public class TestExample {
          @Airflow.TaskHandler(dag = "")
          public void t() {}
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Airflow.TaskHandler on 't' must name the Dag the Python file declares",
    )
  }

  @Test
  @DisplayName("name the registrar of a nested handler class after the classes enclosing it")
  fun generateRegistrarForNestedHandlerClass() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        public class TestExample {
          public static class Inner {
            @Airflow.TaskHandler(dag = "etl", task = "score")
            public long score(long rows) { return rows; }
          }
        }
      """,
      )

    assertThat(compilation).succeeded()
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExample_InnerHandlers")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExample_InnerHandlers",
        """
         package org.apache.airflow.example;

         import java.lang.Exception;
         import java.lang.Long;
         import java.lang.Override;
         import org.apache.airflow.sdk.Bundle;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.internal.TaskArgs;

         /**
          * Registers {@link TestExample.Inner}'s task handlers against the Dags the Python file owns.
          */
         public final class TestExample_InnerHandlers {
           public static void registerInto(Bundle bundle) {
             bundle.register("etl", "score", Score.class);
           }

           public static final class Score implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               TaskArgs args = TaskArgs.of(context, client, 1);
               long rows = args.require(0, Long.class);
               client.setXCom(new TestExample.Inner().score(rows));
             }
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("reject handlers on a nested class that is not static")
  fun rejectHandlersOnInnerClass() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        public class TestExample {
          public class Inner {
            @Airflow.TaskHandler(dag = "etl")
            public void t() {}
          }
        }
      """,
      )

    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Nested class 'Inner' holding @Airflow.TaskHandler methods must be static",
    )
  }

  @Test
  @DisplayName("generate a condition task and a wiring view that names each side")
  fun generateConditionTask() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;

        import org.apache.airflow.sdk.Airflow;

        @Airflow.Dag(id = "etl")
        public class TestExample {
          @Airflow.If(id = "has_rows")
          public boolean hasRows() {
            return true;
          }

          @Airflow.Task
          public void load() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {
              hasRows().Then(load());
            }
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
         import java.util.List;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.ConditionTask;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.internal.DagSource;
         import org.apache.airflow.sdk.internal.Refs;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = DagSource.declaredBy(new DagDef("etl"), TestExample.class);
             return Refs.record(dag, List.of("has_rows", "load"), List.of(), new TestExample.Wiring()::depends);
           }

           public static final class HasRows implements ConditionTask {
             @Override
             public boolean decide(Context context, Client client) throws Exception {
               return new TestExample().hasRows();
             }
           }

           public static final class Load implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               new TestExample().load();
             }
           }
         }
        """,
      )
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleDeps")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleDeps",
        """
         package org.apache.airflow.example;

         import java.lang.Void;
         import org.apache.airflow.sdk.ConditionRef;
         import org.apache.airflow.sdk.Deps;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.TaskRef;
         import org.apache.airflow.sdk.internal.Refs;

         public interface TestExampleDeps extends Deps {
           default ConditionRef hasRows() {
             return ConditionRef.of(Refs.node("", new TaskDef("has_rows", TestExampleBuilder.HasRows.class)));
           }

           default TaskRef<Void> load() {
             return Refs.node("", new TaskDef("load", TestExampleBuilder.Load.class));
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("let a wiring view name a condition's sides in more than one statement")
  fun conditionNamedAcrossStatements() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;

        import org.apache.airflow.sdk.Airflow;

        @Airflow.Dag(id = "etl")
        public class TestExample {
          @Airflow.Task
          public void extract() {}

          @Airflow.If(id = "has_rows")
          public boolean hasRows() {
            return true;
          }

          @Airflow.Task
          public void load() {}

          @Airflow.Task
          public void reportEmpty() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {
              extract().before(hasRows());
              hasRows().Then(load());
              hasRows().Else(reportEmpty());
            }
          }
        }
      """,
      )

    assertThat(compilation).succeeded()
  }

  @Test
  @DisplayName("let a wiring view name a switch's cases in more than one statement")
  fun switchNamedAcrossStatements() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;

        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.Task;

        @Airflow.Dag(id = "etl")
        public class TestExample {
          @Airflow.Switch(id = "pick_path")
          public Class<? extends Task> pickPath() {
            return TestExampleBuilder.HandleLong.class;
          }

          @Airflow.Task
          public void handleLong() {}

          @Airflow.Task
          public void handleShort() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {
              pickPath().Case(handleLong());
              pickPath().Case(handleShort());
            }
          }
        }
      """,
      )

    assertThat(compilation).succeeded()
  }

  @Test
  @DisplayName("generate a switch that names its case by the class generated for it")
  fun generateSwitchTask() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;

        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.Task;

        @Airflow.Dag(id = "etl")
        public class TestExample {
          @Airflow.Switch(id = "pick_path")
          public Class<? extends Task> pickPath() {
            return TestExampleBuilder.HandleLong.class;
          }

          @Airflow.Task
          public void handleLong() {}

          @Airflow.Task
          public void handleShort() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {
              pickPath().Case(handleLong()).Case(handleShort());
            }
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

         import java.lang.Class;
         import java.lang.Exception;
         import java.lang.Override;
         import java.util.List;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.SwitchTask;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.internal.DagSource;
         import org.apache.airflow.sdk.internal.Refs;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = DagSource.declaredBy(new DagDef("etl"), TestExample.class);
             return Refs.record(dag, List.of("pick_path", "handleLong", "handleShort"), List.of(), new TestExample.Wiring()::depends);
           }

           public static final class PickPath implements SwitchTask {
             @Override
             public Class<? extends Task> choose(Context context, Client client) throws Exception {
               return new TestExample().pickPath();
             }
           }

           public static final class HandleLong implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               new TestExample().handleLong();
             }
           }

           public static final class HandleShort implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               new TestExample().handleShort();
             }
           }
         }
        """,
      )
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleDeps")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleDeps",
        """
         package org.apache.airflow.example;

         import java.lang.Void;
         import org.apache.airflow.sdk.Deps;
         import org.apache.airflow.sdk.SwitchRef;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.TaskRef;
         import org.apache.airflow.sdk.internal.Refs;

         public interface TestExampleDeps extends Deps {
           default SwitchRef pickPath() {
             return SwitchRef.of(Refs.node("", new TaskDef("pick_path", TestExampleBuilder.PickPath.class)));
           }

           default TaskRef<Void> handleLong() {
             return Refs.node("", new TaskDef("handleLong", TestExampleBuilder.HandleLong.class));
           }

           default TaskRef<Void> handleShort() {
             return Refs.node("", new TaskDef("handleShort", TestExampleBuilder.HandleShort.class));
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("read a task that triggers a Dag run when the Dag is built")
  fun generateTriggerDagRunTask() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;

        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.TriggerDagRun;

        @Airflow.Dag(id = "etl")
        public class TestExample {
          @Airflow.Task(id = "trigger_downstream", retries = 2)
          public TriggerDagRun triggerDownstream() {
            return new TriggerDagRun("downstream_etl").config("wait_for_completion", true);
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {
              triggerDownstream();
            }
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

         import java.util.List;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.internal.DagSource;
         import org.apache.airflow.sdk.internal.Refs;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = DagSource.declaredBy(new DagDef("etl"), TestExample.class);
             return Refs.record(dag, List.of("trigger_downstream"), List.of(), new TestExample.Wiring()::depends);
           }
         }
        """,
      )
    assertThat(compilation)
      .generatedSourceFile("org.apache.airflow.example.TestExampleDeps")
      .hasSourceEquivalentTo(
        "org.apache.airflow.example.TestExampleDeps",
        """
         package org.apache.airflow.example;

         import java.lang.Void;
         import org.apache.airflow.sdk.Deps;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.TaskRef;
         import org.apache.airflow.sdk.internal.Refs;

         public interface TestExampleDeps extends Deps {
           default TaskRef<Void> triggerDownstream() {
             return Refs.node("", new TaskDef("trigger_downstream", new TestExample().triggerDownstream()).config("retries", 2));
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("build a Dag that reuses one task handle as a condition side and as an upstream")
  fun buildADagThatReusesATaskHandle() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;

        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.TriggerDagRun;

        @Airflow.Dag(id = "etl")
        public class TestExample {
          @Airflow.Task
          public long extract() {
            return 42L;
          }

          @Airflow.Task
          public void load(long rows) {}

          @Airflow.Task
          public void loadEmpty() {}

          @Airflow.If(id = "has_rows")
          public boolean hasRows(long rows) {
            return rows > 0;
          }

          @Airflow.Task(id = "trigger_downstream")
          public TriggerDagRun triggerDownstream() {
            return new TriggerDagRun("downstream_etl");
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {
              var extracted = extract();
              var loaded = load(extracted);
              hasRows(extracted).Then(loaded).Else(loadEmpty());
              loaded.before(triggerDownstream());
            }
          }
        }
      """,
      )

    assertThat(compilation).succeeded()
    // The wiring only runs when the Dag is built, so load it and build it.
    val classes =
      compilation
        .generatedFiles()
        .filter { it.kind == JavaFileObject.Kind.CLASS }
        .associate {
          it.name
            .removePrefix("/CLASS_OUTPUT/")
            .removeSuffix(".class")
            .replace('/', '.') to it.openInputStream().readBytes()
        }
    val loader =
      object : ClassLoader(javaClass.classLoader) {
        override fun findClass(name: String): Class<*> {
          val bytes = classes[name] ?: throw ClassNotFoundException(name)
          return defineClass(name, bytes, 0, bytes.size)
        }
      }

    Bundle().register(loader.loadClass("org.apache.airflow.example.TestExample"))
  }

  @Test
  @DisplayName("reject a task that triggers a Dag run and takes parameters")
  fun rejectTriggerDagRunWithParameters() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.TriggerDagRun;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task
          public TriggerDagRun triggerDownstream(long rows) {
            return new TriggerDagRun("downstream_etl");
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )

    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Airflow.Task method 'triggerDownstream' returns a TriggerDagRun, so it runs when the Dag is " +
        "built rather than when the task runs; it takes no parameters",
    )
  }

  @Test
  @DisplayName("reject a task that triggers a Dag run and throws a checked exception")
  fun rejectTriggerDagRunThrowingCheckedException() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        import org.apache.airflow.sdk.TriggerDagRun;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task
          public TriggerDagRun triggerDownstream() throws java.io.IOException {
            return new TriggerDagRun("downstream_etl");
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )

    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Airflow.Task method 'triggerDownstream' returns a TriggerDagRun, so it runs when the Dag is " +
        "built, where a checked exception cannot be thrown; it must not throw: java.io.IOException",
    )
  }

  @Test
  @DisplayName("reject a switch that does not return the class of a task")
  fun rejectNonTaskClassSwitch() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Switch
          public String pickPath() {
            return "handleLong";
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )

    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Airflow.Switch method 'pickPath' returns java.lang.String, but a switch returns the class of the task it chose",
    )
  }

  @Test
  @DisplayName("reject a switch that returns a class that is not a task")
  fun rejectSwitchReturningNonTaskClass() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Switch
          public Class<String> pickPath() {
            return String.class;
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )

    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Airflow.Switch method 'pickPath' returns java.lang.Class<java.lang.String>, but a switch returns the class of the task it chose",
    )
  }

  @Test
  @DisplayName("reject a condition that does not return a boolean")
  fun rejectNonBooleanCondition() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.If
          public String hasRows() {
            return "yes";
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )

    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Airflow.If method 'hasRows' returns java.lang.String, but a condition returns boolean",
    )
  }

  @Test
  @DisplayName("reject a method that is both a task and a condition")
  fun rejectTaskAndCondition() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag
        public class TestExample {
          @Airflow.Task
          @Airflow.If
          public boolean hasRows() {
            return true;
          }

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {}
          }
        }
      """,
      )

    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Method 'hasRows' carries @Airflow.Task, @Airflow.If; a task is declared by one of them alone",
    )
  }

  @Test
  @DisplayName("map the Dag to the annotated class, not its generated builder")
  fun dagDeclaredByAnnotatedClass() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Airflow;
        @Airflow.Dag(id = "orders")
        public class TestExample {
          @Airflow.Task
          public void t1() {}

          @Airflow.Deps
          static class Wiring implements TestExampleDeps {
            void depends() {
              t1();
            }
          }
        }
      """,
      )
    assertThat(compilation).succeeded()

    val classes =
      compilation
        .generatedFiles()
        .filter { it.kind == JavaFileObject.Kind.CLASS }
        .associate {
          it.name
            .removePrefix("/CLASS_OUTPUT/")
            .removeSuffix(".class")
            .replace('/', '.') to it.openInputStream().readBytes()
        }
    val loader =
      object : ClassLoader(javaClass.classLoader) {
        override fun findClass(name: String): Class<*> {
          val bytes = classes[name] ?: throw ClassNotFoundException(name)
          return defineClass(name, bytes, 0, bytes.size)
        }
      }
    val bundle = Bundle().register(loader.loadClass("org.apache.airflow.example.TestExample"))
    val target = Files.createTempFile("sources", ".json").toFile()
    Server.create(arrayOf("--describe-sources", target.path)).serve(bundle)

    Assertions.assertEquals("""{"orders":"org.apache.airflow.example.TestExample"}""", target.readText())
  }
}
