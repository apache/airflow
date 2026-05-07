package org.apache.airflow.sdk

import com.xenomachina.argparser.ArgParser
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.airflow.sdk.execution.CoordinatorComm
import org.apache.airflow.sdk.execution.LogSender
import org.apache.airflow.sdk.execution.Logger
import kotlin.text.substringAfterLast
import kotlin.text.substringBeforeLast

private class Args(
  parser: ArgParser,
) {
  private fun parseAddress(address: String): InetSocketAddress =
    InetSocketAddress(
      address.substringBeforeLast(':'),
      address.substringAfterLast(':').toInt(),
    )

  val comm by parser.storing("--comm", help = "Address (host:port) to communicate with parent") {
    parseAddress(this)
  }
  val logs by parser.storing("--logs", help = "Address (host:port) to send Airflow logs to") {
    parseAddress(this)
  }
}

class ApiError(
  message: String,
) : IllegalStateException(message)

class Server(
  private val comm: InetSocketAddress,
  private val logs: InetSocketAddress,
) {
  companion object {
    @JvmStatic
    fun create(args: Array<String>): Server {
      val args = ArgParser(args).parseInto(::Args)
      return Server(args.comm, args.logs)
    }
  }

  private val logger = Logger(Server::class)

  fun serve(bundle: Bundle) {
    runBlocking {
      launch {
        awaitAll(
          async {
            aSocket(SelectorManager(Dispatchers.IO)).tcp().connect(comm).use { socket ->
              logger.debug("Connected comm", mapOf("addr" to comm))
              CoordinatorComm(
                bundle,
                socket.openReadChannel(),
                socket.openWriteChannel(autoFlush = true),
              ).startProcessing()
            }
          },
          async {
            aSocket(SelectorManager(Dispatchers.IO)).tcp().connect(logs).use { socket ->
              logger.debug("Connected logs", mapOf("addr" to logs))
              LogSender.configure(socket.openWriteChannel(autoFlush = true))
            }
          },
        )
      }
    }
  }
}
