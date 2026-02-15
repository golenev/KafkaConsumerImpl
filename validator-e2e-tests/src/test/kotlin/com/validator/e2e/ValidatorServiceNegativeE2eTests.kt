package com.validator.e2e

import com.validator.app.model.MissingHeadersPayload
import com.validator.app.model.ValidationPayload
import com.validator.app.model.ValidatedPayload
import com.validator.app.service.KafkaHeaderNames
import com.validator.e2e.kafka.consumer.ConsumerKafkaService
import com.validator.e2e.kafka.consumer.runService
import com.validator.e2e.kafka.producer.ProducerKafkaService
import com.validator.e2e.tests.step
import configs.ObjectMapper
import configs.VALIDATOR_INPUT_TOPIC
import configs.validatorInputProducerConfig
import configs.validatorOutputConsumerConfig
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.util.UUID
import kotlin.random.Random

class ValidatorServiceNegativeE2eTests {

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
    @DisplayName("Проверка, что при входном сообщении с неподдерживаемым typeAction 200 система не публикует результат в выходной топик")
    fun `payload with unsupported typeAction is skipped`() {
        val officeId = Random.nextLong(1, Long.MAX_VALUE)
        val eventId = UUID.randomUUID().toString()

        val payload = step("Сформировать входное сообщение с неподдерживаемым typeAction 200") {
            ValidationPayload(
                eventId = eventId,
                userId = "user-${UUID.randomUUID()}",
                officeId = officeId,
                typeAction = 200,
                status = "NEW",
                sourceSystem = "validator-e2e",
                priority = 5,
                amount = BigDecimal("123.45"),
            )
        }

        val headers = step("Сформировать заголовки для сообщения, которое должно быть отфильтровано") {
            mapOf(
                KafkaHeaderNames.IDEMPOTENCY_KEY to "idem-$eventId",
                KafkaHeaderNames.MESSAGE_ID to "msg-$eventId",
                KafkaHeaderNames.SOURCE_SYSTEM to "validator-e2e-tests"
            )
        }

        step("Отправить сообщение с неподдерживаемым typeAction в input-топик") {
            producer.sendMessageToKafka(eventId, payload, headers)
        }

        val records = step("Дождаться отсутствия сообщений в output-топике для данного officeId") {
            consumer.waitForKeyListAbsent(officeId.toString(), timeoutMs = 30_000)
        }

        step("Проверить что сообщение с неподдерживаемым typeAction не прошло обработку") {
            records.shouldBeEmpty()
        }
    }

    @Test
    @DisplayName("Проверка, что при повторной отправке сообщения с одинаковым idempotency key система обрабатывает только первый экземпляр")
    fun `duplicate payload with same idempotency key is processed only once`() {
        val officeId = Random.nextLong(1, Long.MAX_VALUE)
        val eventId = UUID.randomUUID().toString()

        val payload = step("Сформировать входное сообщение для проверки идемпотентности") {
            ValidationPayload(
                eventId = eventId,
                userId = "user-${UUID.randomUUID()}",
                officeId = officeId,
                typeAction = 100,
                status = "NEW",
                sourceSystem = "validator-e2e",
                priority = 10,
                amount = BigDecimal("555.55"),
            )
        }

        val headers = step("Сформировать заголовки с общим idempotency key для дубликатов") {
            mapOf(
                KafkaHeaderNames.IDEMPOTENCY_KEY to "idem-dedup-$eventId",
                KafkaHeaderNames.MESSAGE_ID to "msg-$eventId",
                KafkaHeaderNames.SOURCE_SYSTEM to "validator-e2e-tests"
            )
        }

        step("Отправить первый экземпляр сообщения с заданным idempotency key") {
            producer.sendMessageToKafka(eventId, payload, headers)
        }

        step("Отправить дублирующий экземпляр сообщения с тем же idempotency key") {
            producer.sendMessageToKafka(eventId, payload, headers)
        }

        val records = step("Дождаться результатов обработки дублирующих сообщений") {
            consumer.waitForKeyListWithHeaders(officeId.toString(), timeoutMs = 35_000, min = 1, max = 2)
        }

        step("Проверить что из двух отправок обработано только одно сообщение") {
            records.shouldHaveSize(1)
            records.first().value.eventId shouldBe eventId
        }

        step("Проверить что у обработанного сообщения сохранён исходный idempotency key") {
            records.first().headers[KafkaHeaderNames.IDEMPOTENCY_KEY] shouldBe headers[KafkaHeaderNames.IDEMPOTENCY_KEY]
        }
    }

    @Test
    @DisplayName("Проверка, что при отсутствии заголовков во входном Kafka-сообщении публикуется JSON с ошибкой и исходным сообщением")
    fun `payload without headers is forwarded as error json with original message`() {
        val officeId = Random.nextLong(1, Long.MAX_VALUE)
        val eventId = UUID.randomUUID().toString()

        val payload = step("Сформировать входное сообщение без Kafka-заголовков") {
            ValidationPayload(
                eventId = eventId,
                userId = "user-${UUID.randomUUID()}",
                officeId = officeId,
                typeAction = 100,
                status = "NEW",
                sourceSystem = "validator-e2e",
                priority = 3,
                amount = BigDecimal("42.00"),
            )
        }

        val missingHeadersConsumer = runService(validatorOutputConsumerConfig(MissingHeadersPayload::class.java)) {
            it.originalMessage.officeId.toString()
        }
        missingHeadersConsumer.start()

        try {
            step("Отправить сообщение в input-топик без заголовков") {
                producer.sendMessageToKafka(eventId, payload, emptyMap())
            }

            val records = step("Дождаться сообщения об отсутствии заголовков в output-топике") {
                missingHeadersConsumer.waitForKeyList(officeId.toString(), timeoutMs = 30_000, min = 1, max = 1)
            }

            val response = step("Выбрать единственное сообщение-ошибку") {
                records.shouldHaveSize(1)
                records.first()
            }

            step("Проверить структуру и содержимое JSON при отсутствии заголовков") {
                response.message shouldBe "Kafka message does not contain headers"

                val original = response.originalMessage
                original.eventId shouldBe payload.eventId
                original.userId shouldBe payload.userId
                original.officeId shouldBe payload.officeId
                original.typeAction shouldBe payload.typeAction
                original.status shouldBe payload.status
                original.sourceSystem shouldBe payload.sourceSystem
                original.priority shouldBe payload.priority
                original.amount shouldBe payload.amount
            }
        } finally {
            missingHeadersConsumer.close()
        }
    }
}
