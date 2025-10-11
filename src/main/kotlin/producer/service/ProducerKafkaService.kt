package producer.service

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.AuthenticationException
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class ProducerKafkaService<T : Any>(
    private val cfg: ProducerKafkaConfig,
    private val topic: String,
    private val mapper: ObjectMapper,
) : AutoCloseable {

    private val logger = LoggerFactory.getLogger(ProducerKafkaService::class.java)
    private val producer: KafkaProducer<String, String>

    init {
        producer = try {
            KafkaProducer(cfg.toProperties())
        } catch (authException: AuthenticationException) {
            logger.error("Failed to initialize Kafka producer for topic {} due to authentication error", topic, authException)
            throw authException
        } catch (ex: Exception) {
            logger.error("Failed to initialize Kafka producer for topic {}", topic, ex)
            throw ex
        }
    }

    fun send(key: String?, payload: T) {
        val json = mapper.writeValueAsString(payload)
        val record = if (key != null) {
            ProducerRecord(topic, key, json)
        } else {
            ProducerRecord(topic, json)
        }

        try {
            producer.send(record).get(30, TimeUnit.SECONDS)
            logger.info("Sent payload with key={} to topic {}: {}", key, topic, json)
        } catch (ex: Exception) {
            val cause = (ex.cause ?: ex)
            if (cause is AuthenticationException) {
                logger.error(
                    "Authentication failed while sending payload with key={} to topic {}",
                    key,
                    topic,
                    cause
                )
            } else {
                logger.error("Failed to send payload with key={} to topic {}: {}", key, topic, json, ex)
            }
            throw ex
        }
    }

    override fun close() {
        producer.use {
            it.flush()
        }
    }
}
