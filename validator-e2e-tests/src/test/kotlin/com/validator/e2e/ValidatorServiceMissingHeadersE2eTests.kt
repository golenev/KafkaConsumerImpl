package com.validator.e2e

import com.validator.app.model.MissingHeadersPayload
import com.validator.app.model.ValidationPayload
import com.validator.e2e.kafka.consumer.ConsumerKafkaService
import com.validator.e2e.kafka.consumer.runService
import com.validator.e2e.kafka.producer.ProducerKafkaService
import com.validator.e2e.tests.step
import configs.ObjectMapper
import configs.VALIDATOR_INPUT_TOPIC
import configs.validatorInputProducerConfig
import configs.validatorOutputConsumerConfig
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.util.*
import kotlin.random.Random

class ValidatorServiceMissingHeadersE2eTests {

    companion object {
        private lateinit var producer: ProducerKafkaService<ValidationPayload>
        private lateinit var missingHeadersConsumer: ConsumerKafkaService<MissingHeadersPayload>
        private val mapper = ObjectMapper.globalMapper

        @JvmStatic
        @BeforeAll
        fun setUp() {
            producer = ProducerKafkaService(
                cfg = validatorInputProducerConfig(),
                topic = VALIDATOR_INPUT_TOPIC,
                mapper = mapper,
            )

            val consumerKafkaConfig = validatorOutputConsumerConfig(MissingHeadersPayload::class.java)

            missingHeadersConsumer = runService(
                cfg = consumerKafkaConfig,
                keySelector = { it.originalMessage.officeId.toString() },
            )
            missingHeadersConsumer.start()
        }

        @JvmStatic
        @AfterAll
        fun tearDown() {
            if (::producer.isInitialized) {
                producer.close()
            }
            if (::missingHeadersConsumer.isInitialized) {
                missingHeadersConsumer.close()
            }
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
    }
}
