package worker

import com.fasterxml.jackson.databind.JsonNode
import java.time.Instant

/**
 * Событие, которое воркер выкладывает в Kafka после обработки POST /create.
 * В тестах мы убеждаемся, что сериализация/десериализация этого объекта работает корректно.
 */
data class WorkerEvent(
    val requestId: String,
    val emittedAt: Instant,
    val payload: JsonNode?
)
