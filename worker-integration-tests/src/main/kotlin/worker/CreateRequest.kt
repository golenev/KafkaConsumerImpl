package worker

import com.fasterxml.jackson.databind.JsonNode

/**
 * Запрос на создание события. Поле [payload] оставляем произвольным JSON,
 * чтобы тесты могли передавать любые дополнительные данные.
 */
data class CreateRequest(
    val requestId: String,
    val payload: JsonNode? = null
)
