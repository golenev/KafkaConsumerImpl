package com.validator.app.service

import com.validator.app.config.ValidatorTopicsProperties
import com.validator.app.model.ValidationPayload
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets
import java.util.UUID

@Component
class InboundEventProducer(
    private val topicsProperties: ValidatorTopicsProperties,
    private val kafkaTemplate: KafkaTemplate<String, ValidationPayload>
) {

    private val logger = LoggerFactory.getLogger(InboundEventProducer::class.java)

    fun publish(payload: ValidationPayload, idempotencyKey: String? = null) {
        val resolvedIdempotencyKey = idempotencyKey?.takeUnless { it.isBlank() } ?: payload.eventId
        val record = ProducerRecord(topicsProperties.input, payload.eventId, payload).apply {
            headers().add(RecordHeader(KafkaHeaderNames.IDEMPOTENCY_KEY, resolvedIdempotencyKey.toByteArray(StandardCharsets.UTF_8)))
            headers().add(RecordHeader(KafkaHeaderNames.MESSAGE_ID, UUID.randomUUID().toString().toByteArray(StandardCharsets.UTF_8)))
            headers().add(RecordHeader(KafkaHeaderNames.SOURCE_SYSTEM, payload.sourceSystem.toByteArray(StandardCharsets.UTF_8)))
        }

        kafkaTemplate.send(record)
        logger.info(
            "Sent payload with eventId={} to topic {} with idempotencyKey={}",
            payload.eventId,
            topicsProperties.input,
            resolvedIdempotencyKey
        )
    }
}
