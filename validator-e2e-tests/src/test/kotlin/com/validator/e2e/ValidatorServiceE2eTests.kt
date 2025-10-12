package com.validator.e2e

import com.validator.app.model.ValidationPayload
import com.validator.app.model.ValidatedPayload
import configs.ValidatorConsumerKafkaSettings
import configs.ValidatorProducerKafkaSettings
import consumer.service.ConsumerKafkaService
import consumer.service.runService
import io.kotest.assertions.throwables.shouldNotThrowAny
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldNotBeBlank
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import producer.service.ProducerKafkaService
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ValidatorServiceE2eTests {

    private val mapper = ValidatorTestObjectMapper.globalMapper

    private lateinit var producer: ProducerKafkaService<ValidationPayload>
    private lateinit var consumer: ConsumerKafkaService<ValidatedPayload>
    private val producerSettings = ValidatorProducerKafkaSettings()
    private val consumerSettings = ValidatorConsumerKafkaSettings()

    @BeforeAll
    fun setUp() {
        val producerConfig = producerSettings.createProducerConfig()

        producer = ProducerKafkaService(
            cfg = producerConfig,
            topic = producerSettings.inputTopic,
            mapper = mapper,
        )

        val consumerConfig = consumerSettings.createConsumerConfig().apply {
            awaitTopic = consumerSettings.outputTopic
            awaitMapper = mapper
            awaitClazz = ValidatedPayload::class.java
            awaitLastNPerPartition = 0
        }

        consumer = runService(consumerConfig) { it.eventId }
        consumer.start()

        // небольшая пауза, чтобы консюмер успел подписаться на топик до начала теста
        Thread.sleep(500)
    }

    @AfterAll
    fun tearDown() {
        if (::producer.isInitialized) {
            producer.close()
        }
        if (::consumer.isInitialized) {
            consumer.close()
        }
    }

    @Test
    fun `payload with typeAction 100 is enriched and forwarded`() {
        val eventId = UUID.randomUUID().toString()
        val payload = ValidationPayload(
            eventId = eventId,
            userId = "user-${UUID.randomUUID()}",
            typeAction = 100,
            status = "NEW",
            sourceSystem = "validator-e2e",
            priority = 7,
            amount = BigDecimal("321.00"),
        )

        producer.send(eventId, payload)

        val records = consumer.waitForKeyList(eventId, timeoutMs = 30_000, min = 1, max = 1)
        records.shouldHaveSize(1)
        val validated = records.first()

        validated.eventId shouldBe payload.eventId
        validated.userId shouldBe payload.userId
        validated.typeAction shouldBe payload.typeAction
        validated.status shouldBe payload.status
        validated.sourceSystem shouldBe payload.sourceSystem
        validated.priority shouldBe payload.priority
        validated.amount shouldBe payload.amount
        validated.validatedAtIso.shouldNotBeBlank()

        val parsedTimestamp = shouldNotThrowAny {
            OffsetDateTime.parse(validated.validatedAtIso)
        }
        parsedTimestamp.shouldNotBeNull()
    }

    @Test
    fun `payload with typeAction 300 produces two output messages`() {
        val eventId = UUID.randomUUID().toString()
        val payload = ValidationPayload(
            eventId = eventId,
            userId = "user-${UUID.randomUUID()}",
            typeAction = 300,
            status = "NEW",
            sourceSystem = "validator-e2e",
            priority = 9,
            amount = BigDecimal("987.65"),
        )

        producer.send(eventId, payload)

        val records = consumer.waitForKeyList(eventId, timeoutMs = 30_000, min = 2, max = 2)
        records.shouldHaveSize(2)
        records.forEach { validated ->
            validated.eventId shouldBe payload.eventId
            validated.userId shouldBe payload.userId
            validated.typeAction shouldBe payload.typeAction
            validated.status shouldBe payload.status
            validated.sourceSystem shouldBe payload.sourceSystem
            validated.priority shouldBe payload.priority
            validated.amount shouldBe payload.amount
            validated.validatedAtIso.shouldNotBeBlank()
            shouldNotThrowAny { OffsetDateTime.parse(validated.validatedAtIso) }.shouldNotBeNull()
        }
    }

    @Test
    fun `payload with unsupported typeAction is skipped`() {
        val eventId = UUID.randomUUID().toString()
        val payload = ValidationPayload(
            eventId = eventId,
            userId = "user-${UUID.randomUUID()}",
            typeAction = 200,
            status = "NEW",
            sourceSystem = "validator-e2e",
            priority = 5,
            amount = BigDecimal("123.45"),
        )

        producer.send(eventId, payload)

        val records = consumer.waitForKeyListAbsent(eventId, timeoutMs = 30_000)
        records.shouldBeEmpty()
    }
}
