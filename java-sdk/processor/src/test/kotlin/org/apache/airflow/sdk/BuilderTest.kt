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
  @DisplayName("generate builder for dag class")
  fun generateBuilderForDagClass() {
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
          public void t1() {}

          @Builder.Task
          public int t2(Client client) {
            return (Integer) client.getXCom("t0");
          }

          @Builder.Task
          public void t3(Context ctx, int value) {
            System.out.println(String.format("%s %s", ctx.ti, value));
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
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.internal.TaskArgs;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = new DagDef("TestExample");
             dag.addTask(new TaskDef("t1", T1.class));
             dag.addTask(new TaskDef("t2", T2.class));
             dag.addTask(new TaskDef("t3", T3.class));
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
               TaskArgs args = TaskArgs.of(context, client, 1);
               int value = args.require(0, Integer.class);
               new TestExample().t3(context, value);
             }
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
         import org.apache.airflow.sdk.internal.TaskArgs;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = new DagDef("TestExample");
             dag.addTask(new TaskDef("t", T.class));
             return dag;
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
         import org.apache.airflow.sdk.internal.TaskArgs;
         import org.apache.airflow.sdk.internal.TypeRef;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = new DagDef("TestExample");
             dag.addTask(new TaskDef("t", T.class));
             return dag;
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
  @DisplayName("bind a TaskInput through the shared populator")
  fun generateBuilderBindsTaskInputFields() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import java.util.List;
        import org.apache.airflow.sdk.ArgName;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.Client;
        import org.apache.airflow.sdk.TaskInput;
        @Builder.Dag
        public class TestExample {
          public static class ScoreInput implements TaskInput {
            @ArgName("region_code") public String region;
            public double threshold;
            public List<String> tags;
          }

          @Builder.Task
          public double score(Client client, ScoreInput input) { return input.threshold; }
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
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.internal.ArgValues;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = new DagDef("TestExample");
             dag.addTask(new TaskDef("score", Score.class));
             return dag;
           }
           public static final class Score implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               TestExample.ScoreInput input = ArgValues.bindInput(client, TestExample.ScoreInput.class);
               client.setXCom(new TestExample().score(client, input));
             }
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
        import org.apache.airflow.sdk.Builder;
        @Builder.Dag
        public class TestExample {
          @Builder.Task
          public void t(String args, int other) {}
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
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.internal.TaskArgs;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = new DagDef("TestExample");
             dag.addTask(new TaskDef("t", T.class));
             return dag;
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
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.TaskInput;
        @Builder.Dag
        public class TestExample {
          public static class ScoreInput implements TaskInput {
            public double threshold;
          }

          @Builder.Task
          public void flat(String client, int context) {}

          @Builder.Task
          public void named(ScoreInput context) {}
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
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.TaskDef;
         import org.apache.airflow.sdk.internal.ArgValues;
         import org.apache.airflow.sdk.internal.TaskArgs;

         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = new DagDef("TestExample");
             dag.addTask(new TaskDef("flat", Flat.class));
             dag.addTask(new TaskDef("named", Named.class));
             return dag;
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
               TestExample.ScoreInput context_ = ArgValues.bindInput(client, TestExample.ScoreInput.class);
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
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.TaskInput;
        @Builder.Dag
        public class TestExample {
          public static class BaseInput {
            private String secret;
          }

          public static class ScoreInput extends BaseInput implements TaskInput {
            public double threshold;
          }

          @Builder.Task
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
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.TaskInput;
        @Builder.Dag
        public class TestExample {
          public static class BaseInput {
            public String regionCode;
          }

          public static class ScoreInput extends BaseInput implements TaskInput {
            public String region_code;
          }

          @Builder.Task
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
  @DisplayName("reject a task declaring more than one TaskInput")
  fun rejectMultipleTaskInputs() {
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
  @DisplayName("reject a TaskInput with a non-public field")
  fun rejectTaskInputWithNonPublicField() {
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
  @DisplayName("reject a TaskInput without a public no-argument constructor")
  fun rejectTaskInputWithoutNoArgConstructor() {
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
         import org.apache.airflow.sdk.DagDef;
         public final class TestExampleBuilder { public static DagDef build() { var dag = new DagDef("foo"); return dag; } }
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
         import org.apache.airflow.sdk.DagDef;
         public final class Foo { public static DagDef build() { var dag = new DagDef("TestExample"); return dag; } }
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
        @Builder.Dag
        public class TestExample { @Builder.Task(id = "foo") public void t1() {} }
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
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.DagDef;
         import org.apache.airflow.sdk.Task;
         import org.apache.airflow.sdk.TaskDef;
         public final class TestExampleBuilder {
           public static DagDef build() {
             var dag = new DagDef("TestExample");
             dag.addTask(new TaskDef("foo", T1.class));
             return dag;
           }
           public static final class T1 implements Task {
             @Override public void execute(Context context, Client client) throws Exception { new TestExample().t1(); }
           }
         }
        """,
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
  @DisplayName("generate a registrar binding each handler to the ids its annotation names")
  fun generateHandlerRegistrar() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        import org.apache.airflow.sdk.Client;
        public class TestExample {
          @Builder.TaskHandler(dag = "etl", task = "score")
          public long score(Client client, long rows) { return rows; }

          @Builder.TaskHandler(dag = "etl")
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
        import org.apache.airflow.sdk.Builder;
        public class TestExample {
          @Builder.TaskHandler(dag = "")
          public void t() {}
        }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "@Builder.TaskHandler on 't' must name the Dag the Python file declares",
    )
  }

  @Test
  @DisplayName("name the registrar of a nested handler class after the classes enclosing it")
  fun generateRegistrarForNestedHandlerClass() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        public class TestExample {
          public static class Inner {
            @Builder.TaskHandler(dag = "etl", task = "score")
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
        import org.apache.airflow.sdk.Builder;
        public class TestExample {
          public class Inner {
            @Builder.TaskHandler(dag = "etl")
            public void t() {}
          }
        }
      """,
      )

    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Nested class 'Inner' holding @Builder.TaskHandler methods must be static",
    )
  }
}
