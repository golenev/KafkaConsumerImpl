package com.validator.app.service

import com.validator.app.config.ValidatorTopicsProperties
import com.validator.app.model.ValidationPayload
import com.validator.app.model.MissingHeadersPayload
import com.validator.app.model.ValidatedPayload
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Service
import java.nio.charset.StandardCharsets
import java.lang.Thread.sleep
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Collections
import java.util.Random
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@Service
class ValidatorService(
    private val topicsProperties: ValidatorTopicsProperties,
    private val kafkaTemplate: KafkaTemplate<String, Any>,
) {

    private val logger = LoggerFactory.getLogger(ValidatorService::class.java)
    private val formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
    private val processedIdempotencyKeys: MutableSet<String> = Collections.newSetFromMap(ConcurrentHashMap())

    @KafkaListener(
        topics = ["\${validator.topics.input}"],
        containerFactory = "validatorKafkaListenerContainerFactory"
    )
    fun onMessage(record: ConsumerRecord<String, ValidationPayload>, acknowledgment: Acknowledgment) {
        val payload = record.value()

        if (record.headers().toArray().isEmpty()) {
            val missingHeadersPayload = MissingHeadersPayload(
                message = "Kafka message does not contain headers",
                originalMessage = payload
            )
            val producerRecord = ProducerRecord(topicsProperties.output, payload.officeId.toString(), missingHeadersPayload as Any)
            kafkaTemplate.send(producerRecord)
            logger.warn("Forwarded missing headers payload for eventId={} to {}", payload.eventId, topicsProperties.output)
            acknowledgment.acknowledge()
            return
        }

        val idempotencyKey = headerOrDefault(record, KafkaHeaderNames.IDEMPOTENCY_KEY, payload.eventId)
        if (!processedIdempotencyKeys.add(idempotencyKey)) {
            logger.info("Skip duplicate payload with eventId={} and idempotencyKey={}", payload.eventId, idempotencyKey)
            acknowledgment.acknowledge()
            return
        }

        val sourceSystem = headerOrDefault(record, KafkaHeaderNames.SOURCE_SYSTEM, payload.sourceSystem)
        val validatedPayloads = when (payload.typeAction) {
            100 -> listOf(createValidatedPayload(payload))
            300 -> createDuplicateValidatedPayloads(payload)
            else -> {
                logger.warn(
                    "Skip payload with eventId={} because typeAction={} does not match validation rule",
                    payload.eventId,
                    payload.typeAction
                )
                acknowledgment.acknowledge()
                return
            }
        }

        if (payload.typeAction == 300) {
            val randomPauseValue = Random().nextLong(10000, 15000)
            sleep(randomPauseValue)
        }

        validatedPayloads.forEachIndexed { index, validated ->
            if (payload.typeAction != 300) {
                val randomPauseValue = Random().nextLong(10000, 15000)
                sleep(randomPauseValue)
            }
            val producerRecord = ProducerRecord(topicsProperties.output, payload.eventId, validated as Any).apply {
                headers().add(
                    RecordHeader(
                        KafkaHeaderNames.IDEMPOTENCY_KEY,
                        idempotencyKey.toByteArray(StandardCharsets.UTF_8)
                    )
                )
                headers().add(
                    RecordHeader(
                        KafkaHeaderNames.MESSAGE_ID,
                        UUID.randomUUID().toString().toByteArray(StandardCharsets.UTF_8)
                    )
                )
                headers().add(
                    RecordHeader(
                        KafkaHeaderNames.SOURCE_SYSTEM,
                        sourceSystem.toByteArray(StandardCharsets.UTF_8)
                    )
                )
                headers().add(
                    RecordHeader(
                        KafkaHeaderNames.PROCESSED_AT,
                        OffsetDateTime.now(ZoneOffset.UTC).format(formatter).toByteArray(StandardCharsets.UTF_8)
                    )
                )
            }
            kafkaTemplate.send(producerRecord)
            logger.info(
                "Validated payload with eventId={} (key={}) forwarded to {} (message {}/{}, idempotencyKey={})",
                validated.eventId,
                payload.eventId,
                topicsProperties.output,
                index + 1,
                validatedPayloads.size,
                idempotencyKey
            )
        }

        acknowledgment.acknowledge()
    }

    private fun createValidatedPayload(payload: ValidationPayload) =
        ValidatedPayload(
            eventId = payload.eventId,
            userId = payload.userId,
            officeId = payload.officeId,
            typeAction = payload.typeAction,
            status = payload.status,
            sourceSystem = payload.sourceSystem,
            priority = payload.priority,
            amount = payload.amount,
            validatedAtIso = OffsetDateTime.now(ZoneOffset.UTC).format(formatter)
        )

    private fun createDuplicateValidatedPayloads(payload: ValidationPayload): List<ValidatedPayload> {
        val primary = createValidatedPayload(payload)
        val secondarySource = payload.copy(
            eventId = "${payload.eventId}-secondary",
            userId = "${payload.userId}-secondary",
            priority = payload.priority + 1
        )
        val secondary = createValidatedPayload(secondarySource)
        return listOf(primary, secondary)
    }

    private fun headerOrDefault(record: ConsumerRecord<String, ValidationPayload>, key: String, default: String): String {
        val header = record.headers().lastHeader(key) ?: return default
        return String(header.value() ?: ByteArray(0), StandardCharsets.UTF_8)
    }
}
