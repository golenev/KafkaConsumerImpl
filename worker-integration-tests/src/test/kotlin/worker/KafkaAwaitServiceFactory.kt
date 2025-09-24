package worker

import consumer.service.ConsumerKafkaConfig
import consumer.service.ConsumerKafkaService
import consumer.service.runService

/**
 * Тестовая обёртка над библиотечным `runService`, чтобы удобно создавать KafkaAwaitService с reified типом.
 */
typealias KafkaAwaitService<T> = ConsumerKafkaService<T>

inline fun <reified T : Any> kafkaAwaitService(
    cfg: ConsumerKafkaConfig,
    noinline keySelector: (T) -> String?
): KafkaAwaitService<T> = runService(cfg, keySelector)
