package worker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.time.Duration
import java.time.Instant
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkerIntegrationTests {

    private val mapper: ObjectMapper = jacksonObjectMapper()
        .registerKotlinModule()
        .registerModule(JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    private val environment = WorkerTestEnvironment(mapper)

    @BeforeAll
    fun setUpAll() {
        environment.startAll()
    }

    @AfterAll
    fun tearDownAll() {
        environment.close()
    }

    @Test
    fun should_publish_event_with_delay() {
        val requestId = "it-${UUID.randomUUID()}"
        val payload = mapper.createObjectNode().put("type", "single")
        val service = environment.awaitService()

        service.start()
        try {
            val requestTime = Instant.now()
            val response = environment.sendCreateRequest(requestId, payload)
            assertEquals(202, response.statusCode())

            val events = service.waitForKeyList(requestId, timeoutMs = 25_000, min = 1, max = 1)
            assertEquals(1, events.size)

            val event = events.single()
            assertEquals(requestId, event.requestId)

            val duration = Duration.between(requestTime, event.emittedAt)
            assertFalse(duration.isNegative, "Событие не может прийти раньше запроса")
            assertTrue(duration.seconds in 5..12, "Ожидали задержку в окне 5-12 секунд, получили $duration")
        } finally {
            service.close()
        }
    }

    @RepeatedTest(3)
    @Execution(ExecutionMode.CONCURRENT)
    fun should_support_parallel_consumption() {
        val requestId = "it-${UUID.randomUUID()}"
        val payload = mapper.createObjectNode().put("type", "parallel")
        val service = environment.awaitService()

        service.start()
        try {
            val response = environment.sendCreateRequest(requestId, payload)
            assertEquals(202, response.statusCode())

            val events = service.waitForKeyList(requestId, timeoutMs = 25_000, min = 1, max = 1)
            assertEquals(1, events.size)
            assertEquals(requestId, events.single().requestId)
        } finally {
            service.close()
        }
    }
}
