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
import kotlin.random.Random

class ValidatorServiceE2eTests {

    companion object {
        private lateinit var producer: ProducerKafkaService<ValidationPayload>
        private lateinit var consumer: ConsumerKafkaService<ValidatedPayload>
        private val producerSettings = ValidatorProducerKafkaSettings()
        private val consumerSettings = ValidatorConsumerKafkaSettings()
        private val mapper = ValidatorTestObjectMapper.globalMapper

        @JvmStatic
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

            consumer = runService(consumerConfig) { it.officeId.toString() }
            consumer.start()

            // небольшая пауза, чтобы консюмер успел подписаться на топик до начала теста
            Thread.sleep(500)
        }

        @JvmStatic
        @AfterAll
        fun tearDown() {
            if (::producer.isInitialized) {
                producer.close()
            }
            if (::consumer.isInitialized) {
                consumer.close()
            }
        }
    }

    @Test
    fun `payload with typeAction 100 is enriched and forwarded`() {
        val officeId = Random.nextLong(1, Long.MAX_VALUE)
        val eventId = UUID.randomUUID().toString()
        val payload = ValidationPayload(
            eventId = eventId,
            userId = "user-${UUID.randomUUID()}",
            officeId = officeId,
            typeAction = 100,
            status = "NEW",
            sourceSystem = "validator-e2e",
            priority = 7,
            amount = BigDecimal("321.00"),
        )

        producer.send(eventId, payload)

        val records = consumer.waitForKeyList(officeId.toString(), timeoutMs = 30_000, min = 1, max = 1)
        records.shouldHaveSize(1)
        val validated = records.first()

        validated.eventId shouldBe payload.eventId
        validated.userId shouldBe payload.userId
        validated.officeId shouldBe payload.officeId
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
        val officeId = Random.nextLong(1, Long.MAX_VALUE)
        val eventId = UUID.randomUUID().toString()
        val payload = ValidationPayload(
            eventId = eventId,
            userId = "user-${UUID.randomUUID()}",
            officeId = officeId,
            typeAction = 300,
            status = "NEW",
            sourceSystem = "validator-e2e",
            priority = 9,
            amount = BigDecimal("987.65"),
        )

        producer.send(eventId, payload)

        val records = consumer.waitForKeyList(officeId.toString(), timeoutMs = 30_000, min = 2, max = 2)
        records.shouldHaveSize(2)
        records.forEach { validated ->
            validated.typeAction shouldBe payload.typeAction
            validated.status shouldBe payload.status
            validated.sourceSystem shouldBe payload.sourceSystem
            validated.amount shouldBe payload.amount
            validated.officeId shouldBe officeId
            validated.validatedAtIso.shouldNotBeBlank()
            shouldNotThrowAny { OffsetDateTime.parse(validated.validatedAtIso) }.shouldNotBeNull()
        }

        val expectedEventIds = setOf(payload.eventId, "${payload.eventId}-secondary")
        records.map { it.eventId }.toSet() shouldBe expectedEventIds

        val expectedUserIds = setOf(payload.userId, "${payload.userId}-secondary")
        records.map { it.userId }.toSet() shouldBe expectedUserIds

        val expectedPriorities = setOf(payload.priority, payload.priority + 1)
        records.map { it.priority }.toSet() shouldBe expectedPriorities
    }

    @Test
    fun `payload with unsupported typeAction is skipped`() {
        val officeId = Random.nextLong(1, Long.MAX_VALUE)
        val eventId = UUID.randomUUID().toString()
        val payload = ValidationPayload(
            eventId = eventId,
            userId = "user-${UUID.randomUUID()}",
            officeId = officeId,
            typeAction = 200,
            status = "NEW",
            sourceSystem = "validator-e2e",
            priority = 5,
            amount = BigDecimal("123.45"),
        )

        producer.send(eventId, payload)

        val records = consumer.waitForKeyListAbsent(officeId.toString(), timeoutMs = 30_000)
        records.shouldBeEmpty()
    }
}
