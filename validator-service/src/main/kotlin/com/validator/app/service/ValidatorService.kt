package com.validator.app.service

import com.validator.app.config.ValidatorTopicsProperties
import com.validator.app.model.ValidationPayload
import com.validator.app.model.ValidatedPayload
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.lang.Thread.sleep
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Random

@Service
class ValidatorService(
    private val topicsProperties: ValidatorTopicsProperties,
    private val kafkaTemplate: KafkaTemplate<String, ValidatedPayload>,
) {

    private val logger = LoggerFactory.getLogger(ValidatorService::class.java)
    private val formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME

    @KafkaListener(
        topics = ["\${validator.topics.input}"],
        containerFactory = "validatorKafkaListenerContainerFactory"
    )
    fun onMessage(payload: ValidationPayload, acknowledgment: Acknowledgment) {
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

        if (validatedPayloads.isNotEmpty()) {
            val randomPauseValue = Random().nextLong(10000, 15000)
            sleep(randomPauseValue)
        }

        validatedPayloads.forEachIndexed { index, validated ->
            kafkaTemplate.send(topicsProperties.output, payload.eventId, validated)
            logger.info(
                "Validated payload with eventId={} (key={}) forwarded to {} (message {}/{})",
                validated.eventId,
                payload.eventId,
                topicsProperties.output,
                index + 1,
                validatedPayloads.size
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
}
