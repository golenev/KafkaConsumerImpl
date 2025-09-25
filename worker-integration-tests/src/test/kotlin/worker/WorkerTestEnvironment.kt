package worker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import consumer.service.ConsumerKafkaConfig
import org.slf4j.LoggerFactory
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
import org.testcontainers.containers.output.Slf4jLogConsumer
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
    private val kafkaAlias = "kafka"

    private val kafka: KafkaContainer = KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.5.3")
    )
        .withNetwork(network)
        .withNetworkAliases(kafkaAlias)

    private val workerImage: ImageFromDockerfile = ImageFromDockerfile()
        .withDockerfileFromBuilder { builder ->
            builder
                .from("eclipse-temurin:17-jre")
                .copy("app/worker.jar", "/app/worker.jar")
                .expose(WORKER_INTERNAL_PORT)
                .entryPoint("java", "-jar", "/app/worker.jar")
                .build()
        }
        .withFileFromPath(
            "app/worker.jar",
            Paths.get("worker-integration-tests", "build", "libs", "worker-app-all.jar")
        )

    private val worker: GenericContainer<*> = GenericContainer(workerImage)
        .withNetwork(network)
        .withExposedPorts(WORKER_INTERNAL_PORT)
        .withEnv("KAFKA_BOOTSTRAP_SERVERS", kafkaBootstrapServersForWorker())
        .withEnv("TOPIC_NAME", TOPIC_NAME)
        .withEnv("PORT", WORKER_INTERNAL_PORT.toString())
        .waitingFor(
            org.testcontainers.containers.wait.strategy.Wait
                .forHttp("/readyz")
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofSeconds(60))
        )
        .withLogConsumer(Slf4jLogConsumer(LoggerFactory.getLogger("worker-container")))

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
        val port = worker.getMappedPort(WORKER_INTERNAL_PORT)
        return URI.create("http://${worker.host}:$port$path")
    }

    /**
     * KafkaContainer отдаёт bootstrap для хоста (PLAINTEXT://localhost:...),
     * а внутри сети Testcontainers сервисам нужен адрес по network alias.
     * Строим строку вручную, сохраняя схему листенера.
     */
    private fun kafkaBootstrapServersForWorker(): String {
        return "PLAINTEXT://$kafkaAlias:$KAFKA_INTERNAL_PORT"
    }

    companion object {
        const val TOPIC_NAME: String = "worker_events"
        private const val KAFKA_INTERNAL_PORT: Int = 9092
        private const val WORKER_INTERNAL_PORT: Int = 18_080
    }
}
