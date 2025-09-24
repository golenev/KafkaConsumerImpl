package worker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import consumer.service.ConsumerKafkaConfig
import consumer.service.KafkaAwaitService
import consumer.service.kafkaAwaitService
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.utility.DockerImageName
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkerIntegrationTests {

    private val mapper: ObjectMapper = jacksonObjectMapper()
        .registerKotlinModule()
        .registerModule(JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    private val network: Network = Network.newNetwork()
    private val kafka: KafkaContainer = KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.5.3")
    ).withNetwork(network)
        .withNetworkAliases("kafka")

    private val workerImage = ImageFromDockerfile()
        .withDockerfileFromBuilder { builder ->
            builder
                .from("eclipse-temurin:17-jre")
                .copy("app/worker.jar", "/app/worker.jar")
                .expose(8080)
                .entryPoint("java", "-jar", "/app/worker.jar")
                .build()
        }
        .withFileFromPath("app/worker.jar", Paths.get("build/libs/worker-app-all.jar"))

    private val worker: GenericContainer<*> = GenericContainer(workerImage)
        .withNetwork(network)
        .withExposedPorts(8080)
        .withEnv("KAFKA_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
        .withEnv("TOPIC_NAME", TOPIC_NAME)
        .waitingFor(
            org.testcontainers.containers.wait.strategy.Wait
                .forHttp("/readyz")
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofSeconds(60))
        )

    private val httpClient: HttpClient = HttpClient.newHttpClient()

    @BeforeAll
    fun setUpAll() {
        kafka.start()
        worker.start()
    }

    @AfterAll
    fun tearDownAll() {
        runCatching { worker.stop() }
        runCatching { kafka.stop() }
        runCatching { network.close() }
    }

    @Test
    fun should_publish_event_with_delay() {
        val requestId = "it-${UUID.randomUUID()}"
        val payload = mapper.createObjectNode().put("type", "single")
        val service = awaitService()

        service.start()
        try {
            val requestTime = Instant.now()
            val response = sendCreateRequest(requestId, payload)
            assertEquals(202, response.statusCode())

            val events = service.waitForKeyList(requestId, timeoutMs = 25_000, min = 1, max = 1)
            assertEquals(1, events.size)

            val event = events.single()
            assertEquals(requestId, event.requestId)

            val duration = Duration.between(requestTime, event.emittedAt)
            assertFalse(duration.isNegative, "Событие не может прийти раньше запроса")
            assertTrue(duration.seconds in 5..12, "Ожидали задержку в окне 5-12 секунд, получили $duration")
        } finally {
            service.close()
        }
    }

    @RepeatedTest(3)
    @Execution(ExecutionMode.CONCURRENT)
    fun should_support_parallel_consumption() {
        val requestId = "it-${UUID.randomUUID()}"
        val payload = mapper.createObjectNode().put("type", "parallel")
        val service = awaitService()

        service.start()
        try {
            val response = sendCreateRequest(requestId, payload)
            assertEquals(202, response.statusCode())

            val events = service.waitForKeyList(requestId, timeoutMs = 25_000, min = 1, max = 1)
            assertEquals(1, events.size)
            assertEquals(requestId, events.single().requestId)
        } finally {
            service.close()
        }
    }

    private fun sendCreateRequest(requestId: String, payload: com.fasterxml.jackson.databind.node.ObjectNode): HttpResponse<String> {
        val body = mapper.writeValueAsString(CreateRequest(requestId = requestId, payload = payload))
        val request = HttpRequest.newBuilder(workerUri("/create"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    }

    private fun workerUri(path: String): URI {
        val port = worker.getMappedPort(8080)
        return URI.create("http://${worker.host}:$port$path")
    }

    private fun awaitService(): KafkaAwaitService<WorkerEvent> {
        val cfg = ConsumerKafkaConfig(
            bootstrapServers = kafka.bootstrapServers,
            username = "",
            password = ""
        ).apply {
            securityProtocol = "PLAINTEXT"
            saslMechanism = "PLAIN"
            groupIdPrefix = "it-${UUID.randomUUID()}-"
            awaitTopic = TOPIC_NAME
            awaitMapper = mapper
            awaitClazz = WorkerEvent::class.java
            awaitLastNPerPartition = 0 // интеграционным тестам нужно ловить только новые сообщения.
        }
        // requestId — надёжный ключ: каждый тест создаёт уникальный идентификатор, чтобы не пересекаться.
        return kafkaAwaitService(cfg) { it.requestId }
    }

    companion object {
        private const val TOPIC_NAME = "worker_events"
    }
}
