package consumer.service

import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class WaitForMany<K, V> {
    private val map = ConcurrentHashMap<K, LinkedBlockingQueue<V>>()
    private val watchers = ConcurrentHashMap<K, CopyOnWriteArraySet<Pair<(V) -> Boolean, CompletableFuture<V?>>>>()
    private val log = LoggerFactory.getLogger(WaitForMany::class.java)

    /**
     * Добавляет элемент [value] в очередь ожидания для ключа [key].
     * Используется консюмером для передачи новых сообщений.
     */
    fun provide(key: K, value: V) {
        watchers[key]?.let { watchersForKey ->
            val delivered = ArrayList<Pair<(V) -> Boolean, CompletableFuture<V?>>>()
            for (registration in watchersForKey) {
                val (predicate, future) = registration
                if (!future.isDone && predicate(value) && future.complete(value)) {
                    delivered.add(registration)
                }
            }
            if (delivered.isNotEmpty()) {
                watchersForKey.removeAll(delivered)
                if (watchersForKey.isEmpty()) {
                    watchers.remove(key, watchersForKey)
                }
            }
        }
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
            log.debug(
                "Polling queue for key={} remainingNs={} collected={}/{}",
                key,
                remNs,
                out.size,
                max
            )
            val v = q.poll(
                TimeUnit.NANOSECONDS.toMillis(remNs.coerceAtLeast(0)),
                TimeUnit.MILLISECONDS
            )
            if (v != null) {
                out.add(v)
                log.debug(
                    "Collected {} item(s) for key={} (required min={}, max={})",
                    out.size,
                    key,
                    min,
                    max
                )
                if (out.size >= min) {
                    log.debug(
                        "Minimum threshold reached for key={} -> returning {} item(s)",
                        key,
                        out.size
                    )
                    return out
                }
            } else {
                // ни одного нового элемента до истечения оставшегося времени
                log.debug(
                    "No new items for key={} before timeout; collected {}/{} so far",
                    key,
                    out.size,
                    min
                )
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
            error("Expected no values for key=$key, but received: $values")
        }
        return emptyList()
    }

    /**
     * Ожидает первое значение для [key], удовлетворяющее условию [predicate].
     * Возвращает найденный элемент либо `null`, если таймаут истёк раньше.
     */
    fun waitForBatch(
        key: K,
        timeoutMs: Long,
        predicate: (V) -> Boolean,
    ): V? {
        map[key]?.let { queue ->
            for (value in queue) {
                if (predicate(value)) {
                    return value
                }
            }
        }

        val future = CompletableFuture<V?>()
        val registration = predicate to future
        val watchersForKey = watchers.computeIfAbsent(key) { CopyOnWriteArraySet() }
        watchersForKey.add(registration)

        map[key]?.let { queue ->
            for (value in queue) {
                if (predicate(value) && future.complete(value)) {
                    watchersForKey.remove(registration)
                    if (watchersForKey.isEmpty()) {
                        watchers.remove(key, watchersForKey)
                    }
                    return value
                }
            }
        }

        val result = try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS)
        } catch (_: TimeoutException) {
            null
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            null
        } finally {
            if (!future.isDone) {
                future.complete(null)
            }
            watchersForKey.remove(registration)
            if (watchersForKey.isEmpty()) {
                watchers.remove(key, watchersForKey)
            }
        }
        return result
    }

    /**
     * Очищает очередь ожидания для указанного ключа [key].
     * Полезно для сброса состояния между тестами.
     */
    fun clear(key: K) {
        map.remove(key)
        watchers.remove(key)?.forEach { (_, future) -> future.complete(null) }
    }
}

