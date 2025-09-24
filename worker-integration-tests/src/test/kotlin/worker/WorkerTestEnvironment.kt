package worker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import consumer.service.ConsumerKafkaConfig
import java.io.Closeable
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Paths
import java.time.Duration
import java.util.UUID
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.utility.DockerImageName

/**
 * Инфраструктурная обёртка, которая поднимает Kafka и worker-сервис в контейнерах
 * и предоставляет утилиты для тестов. Так основные тесты не захламлены техническими
 * деталями запуска.
 */
class WorkerTestEnvironment(
    private val mapper: ObjectMapper
) : Closeable {

    private val network: Network = Network.newNetwork()

    private val kafka: KafkaContainer = KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.5.3")
    )
        .withNetwork(network)
        .withNetworkAliases("kafka")

    private val workerImage: ImageFromDockerfile = ImageFromDockerfile()
        .withDockerfileFromBuilder { builder ->
            builder
                .from("eclipse-temurin:17-jre")
                .copy("app/worker.jar", "/app/worker.jar")
                .expose(8080)
                .entryPoint("java", "-jar", "/app/worker.jar")
                .build()
        }
        .withFileFromPath(
            "app/worker.jar",
            Paths.get("worker-integration-tests", "build", "libs", "worker-app-all.jar")
        )

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

    fun startAll() {
        kafka.start()
        worker.start()
    }

    override fun close() {
        stopAll()
    }

    fun stopAll() {
        runCatching { worker.stop() }
        runCatching { kafka.stop() }
        runCatching { network.close() }
    }

    fun sendCreateRequest(requestId: String, payload: ObjectNode): HttpResponse<String> {
        val body = mapper.writeValueAsString(CreateRequest(requestId = requestId, payload = payload))
        val request = HttpRequest.newBuilder(workerUri("/create"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    }

    fun awaitService(): KafkaAwaitService<WorkerEvent> {
        val cfg = ConsumerKafkaConfig(
            bootstrapServers = kafka.bootstrapServers,
            username = "",
            password = ""
        ).apply {
            securityProtocol = "PLAINTEXT"
            saslMechanism = "PLAIN"
            // Отдельная эфемерная группа на тест гарантирует, что консюмеры не мешают друг другу.
            groupIdPrefix = "it-${UUID.randomUUID()}-"
            awaitTopic = TOPIC_NAME
            awaitMapper = mapper
            awaitClazz = WorkerEvent::class.java
            awaitLastNPerPartition = 0 // интеграционным тестам нужно ловить только новые сообщения из топика.
        }
        // requestId — надёжный ключ для тестов: каждый сценарий использует уникальный идентификатор.
        return kafkaAwaitService(cfg) { it.requestId }
    }

    private fun workerUri(path: String): URI {
        val port = worker.getMappedPort(8080)
        return URI.create("http://${worker.host}:$port$path")
    }

    companion object {
        const val TOPIC_NAME: String = "worker_events"
    }
}
