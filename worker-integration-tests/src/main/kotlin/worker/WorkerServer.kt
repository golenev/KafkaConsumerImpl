package worker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationStopping
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.coroutines.delay
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.Properties
import kotlin.random.Random

private val log = LoggerFactory.getLogger("worker-server")

fun main() {
    val port = System.getenv("PORT")?.toIntOrNull() ?: 8080
    embeddedServer(Netty, port = port) { workerModule() }.start(wait = true)
}

/**
 * Основной модуль Ktor: слушает POST /create, ждёт рандомную задержку и публикует событие в Kafka.
 */
fun Application.workerModule() {
    val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS")
        ?: error("KAFKA_BOOTSTRAP_SERVERS env is required")
    val topicName = System.getenv("TOPIC_NAME") ?: "worker_events"

    val mapper: ObjectMapper = jacksonObjectMapper()
        .registerKotlinModule()
        .registerModule(JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    install(ContentNegotiation) {
        jackson {
            registerKotlinModule()
            registerModule(JavaTimeModule())
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        }
    }

    val producerProps = Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        put(ProducerConfig.ACKS_CONFIG, "all")
        put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    }
    val producer = KafkaProducer<String, String>(producerProps)

    // При остановке приложения аккуратно закрываем продюсер, чтобы не терять сообщения.
    environment.monitor.subscribe(ApplicationStopping) {
        runCatching { producer.flush() }
        producer.close()
    }

    routing {
        get("/readyz") {
            call.respond(HttpStatusCode.OK, mapOf("status" to "ready"))
        }

        post("/create") {
            val createRequest = call.receive<CreateRequest>()

            // Бизнес-логика: воркер не публикует событие мгновенно, а ждёт 5-12 секунд перед отправкой.
            val delaySeconds = Random.nextLong(from = 5, until = 13)
            delay(delaySeconds * 1000)

            val event = WorkerEvent(
                requestId = createRequest.requestId,
                emittedAt = Instant.now(),
                payload = createRequest.payload
            )

            val record = ProducerRecord(
                topicName,
                createRequest.requestId,
                mapper.writeValueAsString(event)
            )
            producer.send(record).get()

            log.info("Event for requestId={} published after {}s", createRequest.requestId, delaySeconds)
            call.respond(HttpStatusCode.Accepted, mapOf("status" to "queued"))
        }
    }
}
