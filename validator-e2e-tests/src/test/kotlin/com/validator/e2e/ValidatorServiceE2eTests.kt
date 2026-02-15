package com.validator.e2e

import com.validator.app.model.ValidationPayload
import com.validator.app.model.ValidatedPayload
import com.validator.app.service.KafkaHeaderNames
import com.validator.e2e.tests.step
import com.validator.e2e.kafka.consumer.ConsumerKafkaService
import com.validator.e2e.kafka.consumer.runService
import com.validator.e2e.kafka.producer.ProducerKafkaService
import configs.VALIDATOR_INPUT_TOPIC
import configs.validatorInputProducerConfig
import configs.validatorOutputConsumerConfig
import configs.ObjectMapper
import io.kotest.assertions.throwables.shouldNotThrowAny
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
import java.util.UUID
import kotlin.random.Random

class ValidatorServiceE2eTests {

    companion object {
        private lateinit var producer: ProducerKafkaService<ValidationPayload>
        private lateinit var consumer: ConsumerKafkaService<ValidatedPayload>
        private val mapper = ObjectMapper.globalMapper

        @JvmStatic
        @BeforeAll
        fun setUp() {
            producer = ProducerKafkaService(
                cfg = validatorInputProducerConfig(),
                topic = VALIDATOR_INPUT_TOPIC,
                mapper = mapper,
            )

            consumer = runService(validatorOutputConsumerConfig(ValidatedPayload::class.java)) { it.officeId.toString() }
            consumer.start()
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
    @DisplayName("Проверка, что при входном сообщении с typeAction 100 система обогащает данные и отправляет одно сообщение в выходной топик")
    fun `payload with typeAction 100 is enriched and forwarded`() {
        val officeId = Random.nextLong(1, Long.MAX_VALUE)
        val eventId = UUID.randomUUID().toString()

        val payload = step("Сформировать входное сообщение для одиночной обработки с typeAction 100") {
            ValidationPayload(
                eventId = eventId,
                userId = "user-${UUID.randomUUID()}",
                officeId = officeId,
                typeAction = 100,
                status = "NEW",
                sourceSystem = "validator-e2e",
                priority = 7,
                amount = BigDecimal("321.00"),
            )
        }

        val headers = step("Сформировать служебные заголовки для трассировки одиночного сообщения") {
            mapOf(
                KafkaHeaderNames.IDEMPOTENCY_KEY to "idem-$eventId",
                KafkaHeaderNames.MESSAGE_ID to "msg-$eventId",
                KafkaHeaderNames.SOURCE_SYSTEM to "validator-e2e-tests"
            )
        }

        step("Отправить входное сообщение в input-топик валидатора") {
            producer.sendMessageToKafka(eventId, payload, headers)
        }

        val records = step("Дождаться единственного сообщения в output-топике после обогащения") {
            consumer.waitForKeyListWithHeaders(officeId.toString(), timeoutMs = 30_000, min = 1, max = 1)
        }

        val consumed = step("Выбрать единственную запись для последующей валидации") {
            records.shouldHaveSize(1)
            records.first()
        }

        step("Проверить перенос бизнес-полей из входного сообщения в выходное") {
            val validated = consumed.value
            validated.eventId shouldBe payload.eventId
            validated.userId shouldBe payload.userId
            validated.officeId shouldBe payload.officeId
            validated.typeAction shouldBe payload.typeAction
            validated.status shouldBe payload.status
            validated.sourceSystem shouldBe payload.sourceSystem
            validated.priority shouldBe payload.priority
            validated.amount shouldBe payload.amount
        }

        step("Проверить наличие и корректность метки времени обогащения") {
            val validated = consumed.value
            validated.validatedAtIso.shouldNotBeBlank()
            shouldNotThrowAny { OffsetDateTime.parse(validated.validatedAtIso) }.shouldNotBeNull()
        }

        step("Проверить корректную передачу и генерацию выходных заголовков") {
            val outboundHeaders = consumed.headers
            outboundHeaders[KafkaHeaderNames.IDEMPOTENCY_KEY] shouldBe headers[KafkaHeaderNames.IDEMPOTENCY_KEY]
            outboundHeaders[KafkaHeaderNames.SOURCE_SYSTEM] shouldBe headers[KafkaHeaderNames.SOURCE_SYSTEM]
            outboundHeaders[KafkaHeaderNames.MESSAGE_ID].shouldNotBeNull().shouldNotBeBlank()
            outboundHeaders[KafkaHeaderNames.PROCESSED_AT].shouldNotBeNull().shouldNotBeBlank()
        }
    }

    @Test
    @DisplayName("Проверка, что при входном сообщении с typeAction 300 система формирует два выходных сообщения с ожидаемыми полями")
    fun `payload with typeAction 300 produces two output messages`() {
        val officeId = Random.nextLong(1, Long.MAX_VALUE)
        val eventId = UUID.randomUUID().toString()

        val payload = step("Сформировать входное сообщение, требующее публикации двух выходных сообщений") {
            ValidationPayload(
                eventId = eventId,
                userId = "user-${UUID.randomUUID()}",
                officeId = officeId,
                typeAction = 300,
                status = "NEW",
                sourceSystem = "validator-e2e",
                priority = 9,
                amount = BigDecimal("987.65"),
            )
        }

        val headers = step("Сформировать заголовки для сценария генерации двух сообщений") {
            mapOf(
                KafkaHeaderNames.IDEMPOTENCY_KEY to "idem-$eventId",
                KafkaHeaderNames.MESSAGE_ID to "msg-$eventId",
                KafkaHeaderNames.SOURCE_SYSTEM to "validator-e2e-tests"
            )
        }

        step("Отправить сообщение, которое должно породить две выходные записи") {
            producer.sendMessageToKafka(eventId, payload, headers)
        }

        val records = step("Дождаться двух выходных сообщений в output-топике") {
            consumer.waitForKeyListWithHeaders(officeId.toString(), timeoutMs = 30_000, min = 2, max = 2)
        }

        step("Проверить количество выходных сообщений в сценарии ветвления") {
            records.shouldHaveSize(2)
        }

        step("Проверить общие бизнес-поля и заголовки у каждого выходного сообщения") {
            records.forEach { consumed ->
                val validated = consumed.value
                validated.typeAction shouldBe payload.typeAction
                validated.status shouldBe payload.status
                validated.sourceSystem shouldBe payload.sourceSystem
                validated.amount shouldBe payload.amount
                validated.officeId shouldBe officeId
                validated.validatedAtIso.shouldNotBeBlank()
                shouldNotThrowAny { OffsetDateTime.parse(validated.validatedAtIso) }.shouldNotBeNull()
                consumed.headers[KafkaHeaderNames.IDEMPOTENCY_KEY] shouldBe headers[KafkaHeaderNames.IDEMPOTENCY_KEY]
                consumed.headers[KafkaHeaderNames.SOURCE_SYSTEM] shouldBe headers[KafkaHeaderNames.SOURCE_SYSTEM]
                consumed.headers[KafkaHeaderNames.MESSAGE_ID].shouldNotBeNull().shouldNotBeBlank()
                consumed.headers[KafkaHeaderNames.PROCESSED_AT].shouldNotBeNull().shouldNotBeBlank()
            }
        }

        step("Проверить формирование основного и secondary eventId") {
            val expectedEventIds = setOf(payload.eventId, "${payload.eventId}-secondary")
            records.map { it.value.eventId }.toSet() shouldBe expectedEventIds
        }

        step("Проверить формирование основного и secondary userId") {
            val expectedUserIds = setOf(payload.userId, "${payload.userId}-secondary")
            records.map { it.value.userId }.toSet() shouldBe expectedUserIds
        }

        step("Проверить формирование исходного и повышенного приоритета") {
            val expectedPriorities = setOf(payload.priority, payload.priority + 1)
            records.map { it.value.priority }.toSet() shouldBe expectedPriorities
        }
    }
}
