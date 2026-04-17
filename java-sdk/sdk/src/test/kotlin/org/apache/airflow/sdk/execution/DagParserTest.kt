package org.apache.airflow.sdk.execution

import org.apache.airflow.sdk.Bundle
import org.apache.airflow.sdk.Dag
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

internal class DagParserTest {
  lateinit var parser: DagParser

  @BeforeEach
  fun setUp() {
    parser = DagParser(":memory:", "")
  }

  @Test
  @DisplayName("Should produce serialized dag")
  fun shouldProduceSerializedDag() {
    val bundle = Bundle("0", listOf(Dag("dag")))
    val result = parser.parse(bundle)
    Assertions.assertEquals(
      DagParsingResult(":memory:", "", bundle.dags),
      result,
    )
  }
}
