package producer.service

import consumer.service.KafkaConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

class ProducerKafkaConfig(
    bootstrapServers: String,
    username: String? = null,
    password: String? = null,
) : KafkaConfig(bootstrapServers, username, password) {

    var clientId: String? = null
    var lingerMs: Long = 0
    var acks: String = "all"
    var enableIdempotence: Boolean = false
    var compressionType: String? = null

    override fun toProperties(): Properties {
        return super.toProperties().apply {
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, acks)
            put(ProducerConfig.LINGER_MS_CONFIG, lingerMs)
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence)
            clientId?.let { put(ProducerConfig.CLIENT_ID_CONFIG, it) }
            compressionType?.let { put(ProducerConfig.COMPRESSION_TYPE_CONFIG, it) }
        }
    }
}
