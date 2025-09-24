package consumer.service

/**
 * Обёртка над [ConsumerKafkaService], чтобы тесты могли создавать сервис ожидания
 * через рефицированную фабрику `kafkaAwaitService`.
 */
typealias KafkaAwaitService<T> = ConsumerKafkaService<T>

inline fun <reified T : Any> kafkaAwaitService(
    cfg: ConsumerKafkaConfig,
    noinline keySelector: (T) -> String?
): KafkaAwaitService<T> = runService(cfg, keySelector)
