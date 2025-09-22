package consumer.service

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.Properties
import java.util.UUID

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsumerKafkaServiceTest {

    companion object {
        private val kafkaImage = DockerImageName.parse("confluentinc/cp-kafka:7.5.0")

        @Container
        @JvmStatic
        val kafka: KafkaContainer = KafkaContainer(kafkaImage)
    }

    private val mapper = jacksonObjectMapper()

    data class TestPayload(val id: String, val payload: String)

    @Test
    fun `assign mode consumes messages by key`() {
        val topic = "assign-${UUID.randomUUID()}"
        kafka.createTopic(topic)

        val cfg = createBaseConfig(topic).apply {
            groupId = null
            groupIdPrefix = null
            autoCommit = false
        }

        runService<TestPayload>(cfg) { it.id }.use { service ->
            service.start()

            produce(topic, TestPayload("assign-key", "payload-1"))

            val result = service.waitForKeyList("assign-key", timeoutMs = 5_000)
            assertEquals(listOf("payload-1"), result.map { it.payload })
        }
    }

    @Test
    fun `subscribe mode with group id consumes new messages`() {
        val topic = "group-${UUID.randomUUID()}"
        kafka.createTopic(topic)

        val cfg = createBaseConfig(topic).apply {
            groupId = "gid-${UUID.randomUUID()}"
            autoCommit = false
        }

        runService<TestPayload>(cfg) { it.id }.use { service ->
            service.start()

            produce(topic, TestPayload("group-key", "payload-2"))

            val result = service.waitForKeyList("group-key", timeoutMs = 5_000)
            assertEquals(listOf("payload-2"), result.map { it.payload })
        }
    }

    @Test
    fun `subscribe mode with group id prefix replays last N records`() {
        val topic = "prefix-${UUID.randomUUID()}"
        kafka.createTopic(topic)

        produce(
            topic,
            TestPayload("prefix-key", "payload-1"),
            TestPayload("prefix-key", "payload-2"),
            TestPayload("prefix-key", "payload-3"),
        )

        val cfg = createBaseConfig(topic).apply {
            groupId = null
            groupIdPrefix = "prefix-${UUID.randomUUID()}-"
            awaitLastNPerPartition = 2
            autoCommit = false
        }

        runService<TestPayload>(cfg) { it.id }.use { service ->
            service.start()

            val result = service.waitForKeyList(
                key = "prefix-key",
                timeoutMs = 5_000,
                min = 2,
                max = 2,
            )

            assertEquals(listOf("payload-2", "payload-3"), result.map { it.payload })
        }
    }

    private fun createBaseConfig(topic: String): ConsumerKafkaConfig {
        return ConsumerKafkaConfig(
            bootstrapServers = kafka.bootstrapServers,
            username = "",
            password = "",
        ).apply {
            securityProtocol = "PLAINTEXT"
            saslMechanism = "PLAIN"
            awaitTopic = topic
            awaitClazz = TestPayload::class.java
            awaitMapper = mapper
            awaitLastNPerPartition = 50
        }
    }

    private fun produce(topic: String, vararg payloads: TestPayload) {
        KafkaProducer<String, String>(producerProps()).use { producer ->
            payloads.forEach { payload ->
                val json = mapper.writeValueAsString(payload)
                producer.send(ProducerRecord(topic, payload.id, json)).get()
            }
        }
    }

    private fun producerProps(): Properties = Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put("security.protocol", "PLAINTEXT")
    }
}
