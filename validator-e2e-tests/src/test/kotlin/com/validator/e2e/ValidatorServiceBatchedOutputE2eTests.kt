package com.validator.e2e

import com.validator.app.model.ValidatedPayload
import com.validator.app.model.ValidationPayload
import com.validator.app.service.KafkaHeaderNames
import com.validator.e2e.kafka.consumer.ConsumerKafkaService
import com.validator.e2e.kafka.consumer.runService
import com.validator.e2e.kafka.producer.ProducerKafkaService
import com.validator.e2e.tests.step
import configs.ObjectMapper
import configs.VALIDATOR_INPUT_TOPIC
import configs.validatorBatchedOutputConsumerConfig
import configs.validatorInputProducerConfig
import io.kotest.assertions.throwables.shouldNotThrowAny
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldNotBeBlank
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.util.*
import kotlin.random.Random

class ValidatorServiceBatchedOutputE2eTests {

    companion object {
        private const val BATCH_KEY = "withBatch"

        private lateinit var producer: ProducerKafkaService<ValidationPayload>
        private lateinit var batchedConsumer: ConsumerKafkaService<ValidatedPayload>
        private val mapper = ObjectMapper.globalMapper

        @JvmStatic
        @BeforeAll
        fun setUp() {
            producer = ProducerKafkaService(
                cfg = validatorInputProducerConfig(),
                topic = VALIDATOR_INPUT_TOPIC,
                mapper = mapper,
            )

            val consumerKafkaConfig = validatorBatchedOutputConsumerConfig(deserializerClass = ValidatedPayload::class.java)

            batchedConsumer = runService(
                cfg = consumerKafkaConfig,
                keySelector = { it.officeId.toString() },
            )
            batchedConsumer.start()
        }

        @JvmStatic
        @AfterAll
        fun tearDown() {
            if (::producer.isInitialized) producer.close()
            if (::batchedConsumer.isInitialized) batchedConsumer.close()
        }
    }

    @Test
    @DisplayName("Проверка, что при ключе withBatch и typeAction 100 сообщение попадает в batched_output")
    fun `payload with typeAction 100 is enriched and forwarded to batched output`() {
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

        step("Отправить одно сообщение с ключом withBatch") {
            val headers = mapOf(
                KafkaHeaderNames.IDEMPOTENCY_KEY to "idem-${payload.eventId}",
                KafkaHeaderNames.MESSAGE_ID to "msg-${payload.eventId}",
                KafkaHeaderNames.SOURCE_SYSTEM to "validator-e2e-tests"
            )
            producer.sendMessageToKafka(BATCH_KEY, payload, headers)
        }

        val records = step("Дождаться сообщения из batched_output") {
            batchedConsumer.waitForKeyListWithHeaders(officeId.toString(), timeoutMs = 30_000, min = 1, max = 1)
        }

        step("Проверить, что целевое сообщение обогащено") {
            records.shouldHaveSize(1)
            val consumed = records.first { it.value.eventId == eventId }
            consumed.value.typeAction shouldBe 100
            consumed.value.status shouldBe "NEW"
            consumed.value.officeId shouldBe officeId
            consumed.value.validatedAtIso.shouldNotBeBlank()
            shouldNotThrowAny { OffsetDateTime.parse(consumed.value.validatedAtIso) }.shouldNotBeNull()
            consumed.headers[KafkaHeaderNames.SOURCE_SYSTEM] shouldBe "validator-e2e-tests"
            consumed.headers[KafkaHeaderNames.MESSAGE_ID].shouldNotBeNull().shouldNotBeBlank()
            consumed.headers[KafkaHeaderNames.PROCESSED_AT].shouldNotBeNull().shouldNotBeBlank()
        }
    }

    @Test
    @DisplayName("Проверка, что при ключе withBatch и typeAction 300 в batched_output формируются две записи на событие")
    fun `payload with typeAction 300 produces two output messages in batched output`() {
        val officeId = Random.nextLong(1, Long.MAX_VALUE)
        val eventId = UUID.randomUUID().toString()

        val payloads = (1..5).map { idx ->
            ValidationPayload(
                eventId = if (idx == 1) eventId else UUID.randomUUID().toString(),
                userId = "user-${UUID.randomUUID()}",
                officeId = officeId,
                typeAction = 300,
                status = "NEW",
                sourceSystem = "validator-e2e",
                priority = 9,
                amount = BigDecimal("987.65"),
            )
        }

        step("Отправить 5 сообщений typeAction=300, чтобы в batched_output получить 10 элементов") {
            payloads.forEach { payload ->
                val headers = mapOf(
                    KafkaHeaderNames.IDEMPOTENCY_KEY to "idem-${payload.eventId}",
                    KafkaHeaderNames.MESSAGE_ID to "msg-${payload.eventId}",
                    KafkaHeaderNames.SOURCE_SYSTEM to "validator-e2e-tests"
                )
                producer.sendMessageToKafka(BATCH_KEY, payload, headers)
            }
        }

        val records = step("Дождаться 10 сообщений (5 * 2) из batched_output") {
            batchedConsumer.waitForKeyListWithHeaders(officeId.toString(), timeoutMs = 35_000, min = 10, max = 10)
        }

        step("Проверить, что для целевого eventId сформированы основная и secondary записи") {
            records.shouldHaveSize(10)
            val target = records.map { it.value }.filter { it.eventId == eventId || it.eventId == "$eventId-secondary" }
            target.shouldHaveSize(2)
            target.map { it.eventId }.toSet() shouldBe setOf(eventId, "$eventId-secondary")
            target.map { it.userId }.toSet().size shouldBe 2
        }
    }

    @Test
    @DisplayName("Проверка, что при ключе withBatch и неподдерживаемом typeAction 200 сообщения не публикуются в batched_output")
    fun `payload with unsupported typeAction is skipped for batched output`() {
        val officeId = Random.nextLong(1, Long.MAX_VALUE)

        val payload = ValidationPayload(
            eventId = UUID.randomUUID().toString(),
            userId = "user-${UUID.randomUUID()}",
            officeId = officeId,
            typeAction = 200,
            status = "NEW",
            sourceSystem = "validator-e2e",
            priority = 5,
            amount = BigDecimal("123.45"),
        )

        val headers = mapOf(
            KafkaHeaderNames.IDEMPOTENCY_KEY to "idem-${payload.eventId}",
            KafkaHeaderNames.MESSAGE_ID to "msg-${payload.eventId}",
            KafkaHeaderNames.SOURCE_SYSTEM to "validator-e2e-tests"
        )

        step("Отправить сообщение с неподдерживаемым typeAction и ключом withBatch") {
            producer.sendMessageToKafka(BATCH_KEY, payload, headers)
        }

        val records = step("Убедиться, что новых сообщений по officeId нет в batched_output") {
            batchedConsumer.waitForKeyListAbsent(officeId.toString(), timeoutMs = 30_000)
        }

        step("Проверить что сообщение не прошло обработку") {
            records.shouldBeEmpty()
        }
    }

    @Test
    @DisplayName("Проверка идемпотентности в batched_output при повторной отправке с одинаковым idempotency key")
    fun `duplicate payload with same idempotency key is processed only once in batched output`() {
        val officeId = Random.nextLong(1, Long.MAX_VALUE)
        val eventId = UUID.randomUUID().toString()
        val duplicateIdempotency = "idem-dedup-$eventId"

        val duplicatePayload = ValidationPayload(
            eventId = eventId,
            userId = "user-${UUID.randomUUID()}",
            officeId = officeId,
            typeAction = 100,
            status = "NEW",
            sourceSystem = "validator-e2e",
            priority = 10,
            amount = BigDecimal("555.55"),
        )

        step("Отправить два дубликата сообщения") {
            val duplicateHeaders = mapOf(
                KafkaHeaderNames.IDEMPOTENCY_KEY to duplicateIdempotency,
                KafkaHeaderNames.MESSAGE_ID to "msg-$eventId",
                KafkaHeaderNames.SOURCE_SYSTEM to "validator-e2e-tests"
            )
            producer.sendMessageToKafka(BATCH_KEY, duplicatePayload, duplicateHeaders)
            producer.sendMessageToKafka(BATCH_KEY, duplicatePayload, duplicateHeaders)
        }

        step("Отправить 9 дополнительных уникальных сообщений для добора батча до 10") {
            repeat(9) {
                val payload = ValidationPayload(
                    eventId = UUID.randomUUID().toString(),
                    userId = "user-${UUID.randomUUID()}",
                    officeId = officeId,
                    typeAction = 100,
                    status = "NEW",
                    sourceSystem = "validator-e2e",
                    priority = 10,
                    amount = BigDecimal("555.55"),
                )
                val headers = mapOf(
                    KafkaHeaderNames.IDEMPOTENCY_KEY to "idem-${payload.eventId}",
                    KafkaHeaderNames.MESSAGE_ID to "msg-${payload.eventId}",
                    KafkaHeaderNames.SOURCE_SYSTEM to "validator-e2e-tests"
                )
                producer.sendMessageToKafka(BATCH_KEY, payload, headers)
            }
        }

        val records = step("Дождаться 10 сообщений в batched_output") {
            batchedConsumer.waitForKeyListWithHeaders(officeId.toString(), timeoutMs = 35_000, min = 10, max = 10)
        }

        step("Проверить что из двух дубликатов обработался только один") {
            records.filter { it.value.eventId == eventId }.shouldHaveSize(1)
        }
    }
}
