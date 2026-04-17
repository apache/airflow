package org.apache.airflow.sdk

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

internal class BundleTest {
  @Test
  @DisplayName("Should index dags by dagId")
  fun shouldIndexDagsByDagId() {
    val dag = Dag("dag")

    val bundle = Bundle("0", listOf(dag))

    Assertions.assertEquals(mapOf("dag" to dag), bundle.dags)
  }

  @Test
  @DisplayName("Should reject duplicate dag ids")
  fun shouldRejectDuplicateDagIds() {
    val error =
      Assertions.assertThrows(IllegalArgumentException::class.java) {
        Bundle("0", listOf(Dag("dag"), Dag("dag")))
      }

    Assertions.assertEquals("Duplicate dagId in bundle: dag", error.message)
  }
}
