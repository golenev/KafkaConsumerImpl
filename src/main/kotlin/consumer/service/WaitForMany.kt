package consumer.service

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class WaitForMany<K, V> {
    private val map = ConcurrentHashMap<K, LinkedBlockingQueue<V>>()

    /**
     * Добавляет элемент [value] в очередь ожидания для ключа [key].
     * Используется консюмером для передачи новых сообщений.
     */
    fun provide(key: K, value: V) {
        map.computeIfAbsent(key) { LinkedBlockingQueue() }.offer(value)
    }

    /**
     * Блокирующе ждёт элементы для key до timeoutMs.
     * Возвращает как только собрано минимум [min] элементов (или таймаут), но не больше [max].
     */
    fun waitMany(
        key: K,
        timeoutMs: Long,
        min: Int = 1,
        max: Int = Int.MAX_VALUE
    ): List<V> {
        require(min >= 0) { "min must be >= 0" }
        require(max >= min) { "max must be >= min" }

        val q = map.computeIfAbsent(key) { LinkedBlockingQueue() }
        val out = ArrayList<V>(min.coerceAtLeast(1))
        val deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)

        while (System.nanoTime() < deadlineNs && out.size < max) {
            val remNs = deadlineNs - System.nanoTime()
            val v = q.poll(
                TimeUnit.NANOSECONDS.toMillis(remNs.coerceAtLeast(0)),
                TimeUnit.MILLISECONDS
            )
            if (v != null) {
                out.add(v)
                if (out.size >= min) return out
            } else {
                // ни одного нового элемента до истечения оставшегося времени
                break
            }
        }
        return out
    }

    /**
     * Ожидает отсутствие элементов для ключа [key] в течение [timeoutMs].
     * Если в очереди появится хотя бы один элемент, возбуждает [IllegalStateException].
     * Возвращает пустой список в случае успешного ожидания.
     */
    fun waitNone(
        key: K,
        timeoutMs: Long,
    ): List<V> {
        val values = waitMany(key, timeoutMs, min = 0, max = 1)
        if (values.isNotEmpty()) {
            throw IllegalStateException("Expected no values for key=$key, but received: $values")
        }
        return emptyList()
    }

    /**
     * Очищает очередь ожидания для указанного ключа [key].
     * Полезно для сброса состояния между тестами.
     */
    fun clear(key: K) {
        map.remove(key)
    }
}

