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

import com.google.testing.compile.Compilation
import com.google.testing.compile.CompilationSubject.assertThat
import com.google.testing.compile.Compiler
import com.google.testing.compile.JavaFileObjectSubject
import com.google.testing.compile.JavaFileObjects
import org.junit.jupiter.api.Assertions.assertEquals
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

private fun Compilation.serverRejectWarnings(): List<String> =
  warnings().map { it.getMessage(null) }.filter { "the Airflow server will reject it" in it }

private fun dagCharsetWarning(id: String) =
  "Dag id \"$id\" must be made of alphanumeric characters, dashes, dots, and underscores; " +
    "the Airflow server will reject it"

private fun dagTooLongWarning(
  id: String,
  length: Int,
) = "Dag id \"$id\" is longer than 250 characters ($length); the Airflow server will reject it"

private fun dagDoubleDotWarning(id: String) =
  "Dag id \"$id\" contains '..'; the Airflow server will reject it unless [core] allow_double_dot_in_ids is enabled"

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
          public void t3(Context ctx, @Builder.XCom(task = "t2") int value) {
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
         import java.lang.Number;
         import java.lang.Override;
         import java.util.Optional;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.Dag;
         import org.apache.airflow.sdk.MissingXComException;
         import org.apache.airflow.sdk.Task;

         public final class TestExampleBuilder {
           public static Dag build() {
             var dag = new Dag("TestExample");
             dag.addTask("t1", T1.class);
             dag.addTask("t2", T2.class);
             dag.addTask("t3", T3.class);
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
               var value = ((Number) Optional.ofNullable(client.getXCom("t2")).orElseThrow(() -> new MissingXComException("t2", "value"))).intValue();
               new TestExample().t3(context, value);
             }
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("widen primitive numerics directly and boxed numerics null-safely")
  fun generateBuilderWidensNumericXCom() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        @Builder.Dag
        public class TestExample {
          @Builder.Task
          public void t(
              @Builder.XCom(task = "a") int i,
              @Builder.XCom(task = "b") long l,
              @Builder.XCom(task = "c") double d,
              @Builder.XCom(task = "f") float fl,
              @Builder.XCom(task = "e") Integer boxedInteger,
              @Builder.XCom(task = "g") Long boxedLong,
              @Builder.XCom(task = "h") Double boxedDouble,
              @Builder.XCom(task = "j") Float boxedFloat) {}
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
         import java.lang.Number;
         import java.lang.Override;
         import java.util.Optional;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.Dag;
         import org.apache.airflow.sdk.MissingXComException;
         import org.apache.airflow.sdk.Task;

         public final class TestExampleBuilder {
           public static Dag build() {
             var dag = new Dag("TestExample");
             dag.addTask("t", T.class);
             return dag;
           }
           public static final class T implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               var i = ((Number) Optional.ofNullable(client.getXCom("a")).orElseThrow(() -> new MissingXComException("a", "i"))).intValue();
               var l = ((Number) Optional.ofNullable(client.getXCom("b")).orElseThrow(() -> new MissingXComException("b", "l"))).longValue();
               var d = ((Number) Optional.ofNullable(client.getXCom("c")).orElseThrow(() -> new MissingXComException("c", "d"))).doubleValue();
               var fl = ((Number) Optional.ofNullable(client.getXCom("f")).orElseThrow(() -> new MissingXComException("f", "fl"))).floatValue();
               var boxedInteger = Optional.ofNullable((Number) client.getXCom("e")).map(Number::intValue).orElse(null);
               var boxedLong = Optional.ofNullable((Number) client.getXCom("g")).map(Number::longValue).orElse(null);
               var boxedDouble = Optional.ofNullable((Number) client.getXCom("h")).map(Number::doubleValue).orElse(null);
               var boxedFloat = Optional.ofNullable((Number) client.getXCom("j")).map(Number::floatValue).orElse(null);
               new TestExample().t(i, l, d, fl, boxedInteger, boxedLong, boxedDouble, boxedFloat);
             }
           }
         }
        """,
      )
  }

  @Test
  @DisplayName("guard non-numeric primitives, leave objects and boxed types nullable")
  fun generateBuilderGuardsNonNumericPrimitiveXCom() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        @Builder.Dag
        public class TestExample {
          @Builder.Task
          public void t(
              @Builder.XCom(task = "a") boolean flag,
              @Builder.XCom(task = "b") String text,
              @Builder.XCom(task = "c") Boolean boxed) {}
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
         import java.lang.Exception;
         import java.lang.Override;
         import java.lang.String;
         import java.util.Optional;
         import org.apache.airflow.sdk.Client;
         import org.apache.airflow.sdk.Context;
         import org.apache.airflow.sdk.Dag;
         import org.apache.airflow.sdk.MissingXComException;
         import org.apache.airflow.sdk.Task;

         public final class TestExampleBuilder {
           public static Dag build() {
             var dag = new Dag("TestExample");
             dag.addTask("t", T.class);
             return dag;
           }
           public static final class T implements Task {
             @Override
             public void execute(Context context, Client client) throws Exception {
               var flag = (Boolean) Optional.ofNullable(client.getXCom("a")).orElseThrow(() -> new MissingXComException("a", "flag"));
               var text = (String) client.getXCom("b");
               var boxed = (Boolean) client.getXCom("c");
               new TestExample().t(flag, text, boxed);
             }
           }
         }
        """,
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
         import org.apache.airflow.sdk.Dag;
         public final class TestExampleBuilder { public static Dag build() { var dag = new Dag("foo"); return dag; } }
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
         import org.apache.airflow.sdk.Dag;
         public final class Foo { public static Dag build() { var dag = new Dag("TestExample"); return dag; } }
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
         import org.apache.airflow.sdk.Dag;
         import org.apache.airflow.sdk.Task;
         public final class TestExampleBuilder {
           public static Dag build() {
             var dag = new Dag("TestExample");
             dag.addTask("foo", T1.class);
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
  @DisplayName("generate builder for dag class with invalid task parameter")
  fun generateBuilderForDagClassWithInvalidTaskParameter() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        @Builder.Dag
        public class TestExample { @Builder.Task(id = "foo") public void t1(String client) {} }
      """,
      )
    assertThat(compilation).failed()
    assertThat(compilation).hadErrorContaining(
      "Unsupported task parameter 'client' with type: java.lang.String",
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
  @DisplayName("dag id warnings — exact messages across every branch")
  fun dagIdWarnings() {
    val astral = "𠀀"
    val tooLongAndInvalid = "a".repeat(250) + " b"
    val cases: List<Pair<String, List<String>>> =
      listOf(
        "simple" to emptyList(),
        "with-dash" to emptyList(),
        "with.dot" to emptyList(),
        "with_underscore" to emptyList(),
        "0numeric" to emptyList(),
        "café_dag" to emptyList(),
        "任務" to emptyList(),
        "a".repeat(250) to emptyList(),
        astral.repeat(250) to emptyList(),
        "a".repeat(251) to listOf(dagTooLongWarning("a".repeat(251), 251)),
        "任".repeat(251) to listOf(dagTooLongWarning("任".repeat(251), 251)),
        astral.repeat(251) to listOf(dagTooLongWarning(astral.repeat(251), 251)),
        "with space" to listOf(dagCharsetWarning("with space")),
        "with/slash" to listOf(dagCharsetWarning("with/slash")),
        "with:colon" to listOf(dagCharsetWarning("with:colon")),
        "with\ttab" to listOf(dagCharsetWarning("with\ttab")),
        "a..b c" to listOf(dagCharsetWarning("a..b c")),
        "a..b" to listOf(dagDoubleDotWarning("a..b")),
        tooLongAndInvalid to listOf(dagTooLongWarning(tooLongAndInvalid, 252), dagCharsetWarning(tooLongAndInvalid)),
      )
    cases.forEach { (id, expected) ->
      val compilation =
        compile(
          """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        @Builder.Dag(id = "$id") public class TestExample {}
        """,
        )
      assertThat(compilation).succeeded()
      assertThat(compilation).generatedSourceFile("org.apache.airflow.example.TestExampleBuilder")
      assertEquals(expected, compilation.serverRejectWarnings(), "id=$id")
    }
  }

  @Test
  @DisplayName("a task warning names its dag")
  fun taskWarningNamesItsDag() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        @Builder.Dag(id = "my_dag")
        public class TestExample { @Builder.Task(id = "bad task") public void t1() {} }
        """,
      )
    assertThat(compilation).succeeded()
    assertEquals(
      listOf(
        "Task id \"bad task\" in dag \"my_dag\" must be made of alphanumeric characters, dashes, dots, and underscores; the Airflow server will reject it",
      ),
      compilation.serverRejectWarnings(),
    )
  }

  @Test
  @DisplayName("a blank id falls back to the element name and does not warn")
  fun blankIdFallsBackToElementName() {
    val compilation =
      compile(
        """
        package org.apache.airflow.example;
        import org.apache.airflow.sdk.Builder;
        @Builder.Dag public class TestExample { @Builder.Task public void t1() {} }
        """,
      )
    assertThat(compilation).succeeded()
    assertEquals(emptyList<String>(), compilation.serverRejectWarnings())
  }
}
