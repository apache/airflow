package org.apache.airflow.sdk

import io.ktor.utils.io.ByteChannel
import io.ktor.utils.io.availableForRead
import io.ktor.utils.io.readAvailable
import kotlinx.coroutines.runBlocking
import org.apache.airflow.sdk.execution.CoordinatorComm
import org.apache.airflow.sdk.execution.DagFileParseRequest
import org.apache.airflow.sdk.execution.IncomingFrame
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.text.split

fun byteArrayFromHexString(hexString: String): ByteArray =
  hexString
    .split(' ', '\r', '\n')
    .filter { it.isNotEmpty() }
    .map { it.toUByte(16).toByte() }
    .toByteArray()

@OptIn(ExperimentalUnsignedTypes::class)
internal class CoordinatorCommTest {
  lateinit var comm: CoordinatorComm
  lateinit var reader: ByteChannel
  lateinit var writer: ByteChannel

  @BeforeEach
  fun setUp() {
    reader = ByteChannel(autoFlush = true)
    writer = ByteChannel(autoFlush = true)
    comm = CoordinatorComm(Bundle("0", listOf(Dag("dag"))), reader, writer)
  }

  @Test
  @DisplayName("handleIncoming should produce parse result")
  fun handleIncomingShouldProduceParseResult() {
    val frame = IncomingFrame(0, DagFileParseRequest().apply { file = ":memory:" })

    // prefix + DagFileParsingResult payload for a minimal DAG.

    /* prefix +
    [
      0,
      {
        "type": "DagFileParsingResult",
        "fileloc": ":memory:",
        "serialized_dags": [
          {
            "data": {
              "__version": 3,
              "dag": {
                "dag_id": "dag",
                "fileloc": ":memory:",
                "relative_fileloc": ".",
                "timezone": "UTC",
                "timetable": {
                  "__type": "airflow.timetables.simple.NullTimetable",
                  "__var": {}
                },
                "tasks": []
              }
            }
          }
        ]
      }
    ]
     */
    val expected =
      byteArrayFromHexString(
        """
      | 00 00 01 e6
      | 92 00 83 a4 74 79 70 65 b4 44 61 67 46 69 6c 65 50 61 72 73 69 6e 67 52
      | 65 73 75 6c 74 a7 66 69 6c 65 6c 6f 63 a8 3a 6d 65 6d 6f 72 79 3a af 73
      | 65 72 69 61 6c 69 7a 65 64 5f 64 61 67 73 91 81 a4 64 61 74 61 82 a9 5f
      | 5f 76 65 72 73 69 6f 6e 03 a3 64 61 67 8c a6 64 61 67 5f 69 64 a3 64 61
      | 67 a7 66 69 6c 65 6c 6f 63 a8 3a 6d 65 6d 6f 72 79 3a b0 72 65 6c 61 74
      | 69 76 65 5f 66 69 6c 65 6c 6f 63 a1 2e a8 74 69 6d 65 7a 6f 6e 65 a3 55
      | 54 43 a9 74 69 6d 65 74 61 62 6c 65 82 a6 5f 5f 74 79 70 65 d9 27 61 69
      | 72 66 6c 6f 77 2e 74 69 6d 65 74 61 62 6c 65 73 2e 73 69 6d 70 6c 65 2e
      | 4e 75 6c 6c 54 69 6d 65 74 61 62 6c 65 a5 5f 5f 76 61 72 80 a5 74 61 73
      | 6b 73 90 b0 64 61 67 5f 64 65 70 65 6e 64 65 6e 63 69 65 73 90 aa 74 61
      | 73 6b 5f 67 72 6f 75 70 8b a9 5f 67 72 6f 75 70 5f 69 64 c0 b2 67 72 6f
      | 75 70 5f 64 69 73 70 6c 61 79 5f 6e 61 6d 65 a0 af 70 72 65 66 69 78 5f
      | 67 72 6f 75 70 5f 69 64 c3 a7 74 6f 6f 6c 74 69 70 a0 a8 75 69 5f 63 6f
      | 6c 6f 72 ae 43 6f 72 6e 66 6c 6f 77 65 72 42 6c 75 65 aa 75 69 5f 66 67
      | 63 6f 6c 6f 72 a4 23 30 30 30 a8 63 68 69 6c 64 72 65 6e 80 b2 75 70 73
      | 74 72 65 61 6d 5f 67 72 6f 75 70 5f 69 64 73 90 b4 64 6f 77 6e 73 74 72
      | 65 61 6d 5f 67 72 6f 75 70 5f 69 64 73 90 b1 75 70 73 74 72 65 61 6d 5f
      | 74 61 73 6b 5f 69 64 73 90 b3 64 6f 77 6e 73 74 72 65 61 6d 5f 74 61 73
      | 6b 5f 69 64 73 90 a9 65 64 67 65 5f 69 6e 66 6f 80 a6 70 61 72 61 6d 73
      | 90 a8 64 65 61 64 6c 69 6e 65 c0 b1 61 6c 6c 6f 77 65 64 5f 72 75 6e 5f
      | 74 79 70 65 73 c0
        """.trimMargin(),
      )

    val buffer = ByteArray(1024) { 0 } // Change ByteArray size if assertTrue below fails.
    var count = 0
    runBlocking {
      comm.handleIncoming(frame)
      if (writer.availableForRead > 0) {
        count = writer.readAvailable(buffer)
      }
    }
    Assertions.assertTrue(count < buffer.size, "Please increase buffer size above")

    Assertions.assertEquals(expected.size, count)

    val received = buffer.sliceArray(0.until(count))
    Assertions.assertArrayEquals(expected, received)
  }
}
