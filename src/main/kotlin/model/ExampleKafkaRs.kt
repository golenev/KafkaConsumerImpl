package com.model

import java.time.LocalDate
import java.time.OffsetDateTime

data class ExampleKafkaRs(
    val id: Long,
    val employeeId: Long,
    val officeId: Long,
    val contract: Contract,
    val employeeType: Int,
    val endOfInternshipDate: LocalDate,
    val paymentDate: LocalDate,
    val officeDt: OffsetDateTime
) {
    data class Contract(
        val mentorId: Long,
        val traineeId: Long,
        val internshipDate: LocalDate
    )
}
