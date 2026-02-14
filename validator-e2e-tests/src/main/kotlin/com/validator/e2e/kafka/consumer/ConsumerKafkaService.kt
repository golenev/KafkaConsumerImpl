package com.validator.e2e.kafka.consumer

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.LoggerFactory
import java.time.Duration
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

data class ConsumedMessage<T>(
    val value: T,
    val headers: Map<String, String>,
)

class ConsumerKafkaService<T : Any>(
    private val cfg: ConsumerKafkaConfig,
    /**Тип данных для мапинга сообщений. Передаётся фабрикой с reified T. */
    private val clazz: Class<T>,
    private val keySelector: (T) -> String?,
) : Runnable, AutoCloseable {

    private val log = LoggerFactory.getLogger(ConsumerKafkaService::class.java)

    /**
     * Флаг синхронизации ресурсов на случай конфигурации @BeforeAll + PER_CLASS,
     * когда один консюмер сервис и много тестов.
     * Для BeforeEach бесполезен
     */
    private val isRunningNow = AtomicBoolean(false)
    private val waiter = WaitForMany<String, T>()
    private val waiterWithHeaders = WaitForMany<String, ConsumedMessage<T>>()
    private lateinit var consumer: KafkaConsumer<String, String>
    private var threadRef: Thread? = null
    private val topic: String = cfg.awaitTopic
    private val mapper: ObjectMapper = cfg.awaitMapper
    private val lastNPerPartition: Int = cfg.awaitLastNPerPartition

    /**
     * Запускает отдельный поток и начинает чтение сообщений из Kafka.
     * Если сервис уже запущен, повторный вызов ничего не делает.
     */
    fun start() {
        if (isRunningNow.getAndSet(true)) return

        val props: Properties = cfg.toProperties().apply {
            putIfAbsent(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
            )
            putIfAbsent(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
            )
            putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        }

        consumer = try {
            KafkaConsumer(props)
        } catch (authException: AuthenticationException) {
            isRunningNow.set(false)
            log.error("Failed to initialize Kafka consumer for topic {} due to authentication error", topic, authException)
            throw authException
        } catch (ex: Exception) {
            isRunningNow.set(false)
            log.error("Failed to initialize Kafka consumer for topic {}", topic, ex)
            throw ex
        }
        threadRef = thread(name = "kafka-await-$topic", start = true) { run() }
    }


    /**
     * Основной цикл чтения сообщений из Kafka.
     * В зависимости от настроек — либо через `subscribe`, либо через `assign`.
     * Парсит JSON в объекты типа [T] и кладёт их в очередь ожидания.
     */
    override fun run() {
        try {
            if (hasGroupId()) {
                consumer.subscribe(listOf(topic))
                waitForAssignment()
                positionToTailOrLastN(consumer.assignment().toList())
            } else {
                val infos = consumer.partitionsFor(topic) ?: emptyList()
                val tps = infos.map { TopicPartition(topic, it.partition()) }
                consumer.assign(tps)
                positionToTailOrLastN(tps)
            }

            while (isRunningNow.get()) {
                val records = consumer.poll(Duration.ofMillis(300))
                if (!records.isEmpty) {
                    records.forEach {
                        try {
                            val value: T = mapper.readValue(it.value(), clazz)
                            val k: String? = keySelector(value)
                            if (k != null) {
                                waiter.provide(k, value)
                                val headers = it.headers().associate { header ->
                                    header.key() to String(header.value() ?: ByteArray(0), StandardCharsets.UTF_8)
                                }
                                waiterWithHeaders.provide(k, ConsumedMessage(value, headers))
                            }
                        } catch (e: JsonProcessingException) {
                            log.warn("JSON parse error at ${it.topic()}-${it.partition()}@${it.offset()}: ${e.message}")
                        }
                    }
                    try {
                        if (hasGroupId()) consumer.commitSync()
                    } catch (e: CommitFailedException) {
                        log.warn("Commit failed: ${e.message}")
                    }
                }
            }
        } catch (e: AuthenticationException) {
            log.error("Kafka authentication error while consuming from {}", topic, e)
            throw e
        } catch (e: WakeupException) {
            // ожидаемый путь при остановке: wakeup() прерывает poll()
            if (isRunningNow.get()) log.error("Unexpected wakeup while consuming", e)
        } catch (e: KafkaException) {
            // другие ошибки уровня Kafka (network, coordinator и т.п.)
            if (isRunningNow.get()) log.error("Kafka error while consuming", e)
        } finally {
            try {
                consumer.close()
            } catch (_: Exception) {
            }
        }
    }

    /**
     * Блокирующе ждёт список объектов по заданному [key].
     *
     * @param key ключ для поиска
     * @param timeoutMs максимальное время ожидания в миллисекундах
     * @param min минимальное количество элементов, которое нужно дождаться
     * @param max максимальное количество элементов, которые можно вернуть
     * @return список полученных объектов (может быть пустым при таймауте)
     */
    fun waitForKeyList(
        key: String,
        timeoutMs: Long = 40_000,
        min: Int = 1,
        max: Int = Int.MAX_VALUE,
    ): List<T> = waiter.waitMany(key, timeoutMs, min, max)

    /**
     * Ожидает отсутствие сообщений для указанного [key] в течение [timeoutMs].
     * Если сообщение появилось, возбуждает [IllegalStateException].
     * Возвращает пустой список для удобства негативных проверок.
     */
    fun waitForKeyListAbsent(
        key: String,
        timeoutMs: Long = 40_000,
    ): List<T> = waiter.waitNone(key, timeoutMs)

    fun waitForKeyListWithHeaders(
        key: String,
        timeoutMs: Long = 40_000,
        min: Int = 1,
        max: Int = Int.MAX_VALUE,
    ): List<ConsumedMessage<T>> = waiterWithHeaders.waitMany(key, timeoutMs, min, max)

    /**
     * Останавливает цикл чтения из Kafka и завершает поток.
     * Повторные вызовы игнорируются.
     */
    private fun stop() {
        if (!isRunningNow.getAndSet(false)) return
        try {
            consumer.wakeup()
        } catch (_: Exception) {
        }
        threadRef?.join(2000)
    }

    /**
     * Реализация интерфейса [AutoCloseable].
     * Делегирует вызов в [stop].
     */
    override fun close() = stop()

    /**
     * Устанавливает позицию чтения в конец топика либо на N последних сообщений.
     * Используется при инициализации консюмера.
     *
     * @param tps список партиций для позиционирования
     */
    private fun positionToTailOrLastN(tps: List<TopicPartition>) {
        if (tps.isEmpty()) return
        if (lastNPerPartition <= 0) {
            consumer.seekToEnd(tps)
            return
        }
        val begin = consumer.beginningOffsets(tps.toSet())
        val end = consumer.endOffsets(tps.toSet())

        tps.forEach {
            val e = end[it] ?: 0L
            val b = begin[it] ?: 0L
            val n = lastNPerPartition.toLong().coerceAtLeast(1L)
            val start = maxOf(b, e - n)
            consumer.seek(it, start)
        }
    }

    /**
     * Ожидает назначения партиций при работе в режиме `subscribe`.
     * Если партиции не назначены в течение 5 секунд — пишет предупреждение в лог.
     */
    private fun waitForAssignment() {
        var assigned = consumer.assignment()
        val deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos()
        while (assigned.isEmpty() && System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(50))
            assigned = consumer.assignment()
        }
        if (assigned.isEmpty()) {
            log.warn("No partitions assigned for topic=$topic (subscribe mode)")
        }
    }

    /**
     * Проверяет, задан ли `groupId` или `groupIdPrefix`.
     * Это определяет, будет ли консюмер работать в режиме `subscribe` или `assign`.
     */
    private fun hasGroupId(): Boolean =
        (cfg.groupId != null) || (cfg.groupIdPrefix != null)
}

/**
 * Точка входа для создания экземпляра сервиса
 */
inline fun <reified T : Any> runService(
    cfg: ConsumerKafkaConfig,
    noinline keySelector: (T) -> String?,
): ConsumerKafkaService<T> {
    return ConsumerKafkaService(
        cfg = cfg,
        clazz = T::class.java,
        keySelector = keySelector
    )
}
