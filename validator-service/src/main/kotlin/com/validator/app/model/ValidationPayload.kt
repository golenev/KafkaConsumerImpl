package com.validator.app.model

import com.fasterxml.jackson.annotation.JsonProperty
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotNull
import java.math.BigDecimal

/**
 * Модель входящего события, которое отправляется продюсером в топик `in_validator`.
 */
data class ValidationPayload(
    @field:NotBlank
    val eventId: String,
    @field:NotBlank
    val userId: String,
    @field:NotNull
    @JsonProperty("type_action")
    val typeAction: Int,
    @field:NotBlank
    val status: String,
    @field:NotBlank
    val sourceSystem: String,
    @field:Min(0)
    val priority: Int,
    val amount: BigDecimal?
)

/**
 * Модель события, которое попадает в топик `out_validator` после успешной проверки.
 */
data class ValidatedPayload(
    val eventId: String,
    val userId: String,
    @JsonProperty("type_action")
    val typeAction: Int,
    val status: String,
    val sourceSystem: String,
    val priority: Int,
    val amount: BigDecimal?,
    val validatedAtIso: String
)
