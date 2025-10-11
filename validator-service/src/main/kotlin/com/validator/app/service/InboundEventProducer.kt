package com.validator.app.service

import com.validator.app.config.ValidatorTopicsProperties
import com.validator.app.model.ValidationPayload
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class InboundEventProducer(
    private val topicsProperties: ValidatorTopicsProperties,
    private val kafkaTemplate: KafkaTemplate<String, ValidationPayload>
) {

    private val logger = LoggerFactory.getLogger(InboundEventProducer::class.java)

    fun publish(payload: ValidationPayload) {
        kafkaTemplate.send(topicsProperties.input, payload.eventId, payload)
        logger.info("Sent payload with eventId={} to topic {}", payload.eventId, topicsProperties.input)
    }
}
