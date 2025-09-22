package consumer.service

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.*

class ConsumerKafkaConfig(
    bootstrapServers: String,
    username: String,
    password: String
) : KafkaConfig(bootstrapServers, username, password) {

    // делаем публичными — чтобы можно было задавать снаружи
    var groupIdPrefix: String? = null
    var groupId: String? = null
    var groupInstanceId: String? = null
    var autoCommit: Boolean = true
    var autoOffsetReset: String = "earliest"
    var partitionAssignmentStrategy: String =
        """org.apache.kafka.clients.consumer.RangeAssignor,org.apache.kafka.clients.consumer.CooperativeStickyAssignor"""
    lateinit var awaitTopic: String
    lateinit var awaitClazz: Class<*>
    var awaitLastNPerPartition: Int = 50
    lateinit var awaitMapper: ObjectMapper

    override fun toProperties(): Properties {
        return super.toProperties().apply {
            put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer::class.java.name
            )
            put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer::class.java.name
            )
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
            put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStrategy)

            if (!autoCommit) {
                put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            }

            groupIdPrefix?.let {
                put(ConsumerConfig.GROUP_ID_CONFIG, "$it${UUID.randomUUID()}")
            } ?: groupId?.let {
                put(ConsumerConfig.GROUP_ID_CONFIG, it)
            }

            groupInstanceId?.let {
                put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, it)
            }
        }
    }
}
