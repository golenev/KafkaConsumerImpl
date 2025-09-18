package com.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.time.Duration

class KafkaMessageFinder(
    @PublishedApi internal val config: KafkaConsumerConfig,
    @PublishedApi internal val mapper: ObjectMapper = defaultMapper()
) {

    /**
     * Ищем запись с заданным key в topic и парсим value как JSON в тип T.
     * Работает и с group.id (subscribe), и без group.id (assign).
     *
     * @param topic топик
     * @param key искомый ключ
     * @param fromBeginning если true, читаем с начала; иначе с конца
     * @param timeout общий таймаут поиска
     * @return распарсенный объект типа T или null, если не нашли
     */
    inline fun <reified T> findByKey(
        topic: String,
        key: String,
        fromBeginning: Boolean = true,
        timeout: Duration = Duration.ofSeconds(15)
    ): T? {
        // Создаём новый consumer на каждый вызов: KafkaConsumer не потокобезопасен
        val props = config.toProperties().apply {
            if (!containsKey("enable.auto.commit")) {
                put("enable.auto.commit", "false")
            }
        }

        KafkaConsumer<String, String>(props).use { consumer ->
            val hasGroup = props.containsKey("group.id")

            if (hasGroup) {
                // Подписка — оффсеты группы будут учитываться (если включишь коммиты)
                consumer.subscribe(listOf(topic))
            } else {
                // Без group.id — статичный “тап”: assign + seek
                val partitionsInfo = consumer.partitionsFor(topic) ?: return null
                val tps = partitionsInfo.map { TopicPartition(topic, it.partition()) }
                consumer.assign(tps)
                if (fromBeginning) consumer.seekToBeginning(tps) else consumer.seekToEnd(tps)
            }

            val deadline = System.nanoTime() + timeout.toNanos()
            while (System.nanoTime() < deadline) {
                val records = consumer.poll(Duration.ofMillis(300))
                if (!records.isEmpty) {
                    for (rec in records) {
                        if (rec.key() == key) {
                            return try {
                                mapper.readValue<T>(rec.value())
                            } catch (e: Exception) {
                                throw IllegalStateException("JSON parse error for key='$key' in topic='$topic'", e)
                            }
                        }
                    }
                } else {
                    Thread.sleep(25)
                }
            }
        }
        return null
    }

    companion object {
        fun defaultMapper(): ObjectMapper =
            ObjectMapper().registerModule(KotlinModule.Builder().build())
    }
}