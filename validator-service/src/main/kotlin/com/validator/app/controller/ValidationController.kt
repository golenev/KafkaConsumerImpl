package com.validator.app.controller

import com.validator.app.model.ValidationPayload
import com.validator.app.service.InboundEventProducer
import jakarta.validation.Valid
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1/validation")
class ValidationController(
    private val inboundEventProducer: InboundEventProducer
) {

    @PostMapping
    fun publish(@Valid @RequestBody payload: ValidationPayload): ResponseEntity<Void> {
        inboundEventProducer.publish(payload)
        return ResponseEntity.accepted().build()
    }
}
