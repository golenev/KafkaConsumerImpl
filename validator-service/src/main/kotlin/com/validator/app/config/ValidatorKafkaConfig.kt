package com.validator.app.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.validator.app.model.ValidationPayload
import com.validator.app.model.ValidatedPayload
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.util.backoff.FixedBackOff
import java.util.UUID

@Configuration
@EnableKafka
@EnableConfigurationProperties(value = [ValidatorTopicsProperties::class, ValidatorKafkaProperties::class])
class ValidatorKafkaConfig {

    private val logger = LoggerFactory.getLogger(ValidatorKafkaConfig::class.java)

    @Bean
    fun objectMapper(): ObjectMapper = ObjectMapper()
        .registerKotlinModule()
        .registerModule(JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    @Bean
    fun kafkaAdmin(kafkaProperties: ValidatorKafkaProperties): KafkaAdmin {
        val configs = mutableMapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaProperties.bootstrapServers
        )

        configs.applySecurity(kafkaProperties)

        return KafkaAdmin(configs)
    }

    @Bean
    fun validationPayloadProducerFactory(
        kafkaProperties: ValidatorKafkaProperties,
        objectMapper: ObjectMapper
    ): ProducerFactory<String, ValidationPayload> {
        val props = baseProducerProps(kafkaProperties)
        return DefaultKafkaProducerFactory(props, StringSerializer(), JsonSerializer<ValidationPayload>(objectMapper).apply {
            setAddTypeInfo(false)
        })
    }

    @Bean
    fun validatedPayloadProducerFactory(
        kafkaProperties: ValidatorKafkaProperties,
        objectMapper: ObjectMapper
    ): ProducerFactory<String, ValidatedPayload> {
        val props = baseProducerProps(kafkaProperties)
        return DefaultKafkaProducerFactory(props, StringSerializer(), JsonSerializer<ValidatedPayload>(objectMapper).apply {
            setAddTypeInfo(false)
        })
    }

    private fun baseProducerProps(kafkaProperties: ValidatorKafkaProperties): Map<String, Any> {
        return mutableMapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaProperties.bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all"
        ).apply {
            applySecurity(kafkaProperties)
        }
    }

    @Bean
    fun validationKafkaTemplate(
        validationPayloadProducerFactory: ProducerFactory<String, ValidationPayload>
    ): KafkaTemplate<String, ValidationPayload> = KafkaTemplate(validationPayloadProducerFactory)

    @Bean
    fun validatedKafkaTemplate(
        validatedPayloadProducerFactory: ProducerFactory<String, ValidatedPayload>
    ): KafkaTemplate<String, ValidatedPayload> = KafkaTemplate(validatedPayloadProducerFactory)

    @Bean
    fun validationConsumerFactory(
        kafkaProperties: ValidatorKafkaProperties,
        objectMapper: ObjectMapper
    ): ConsumerFactory<String, ValidationPayload> {
        val props = mutableMapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaProperties.bootstrapServers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to JsonDeserializer::class.java,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to kafkaProperties.autoOffsetReset,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false
        )

        props.applySecurity(kafkaProperties)

        when {
            kafkaProperties.groupIdPrefix != null -> {
                val groupId = kafkaProperties.groupIdPrefix + UUID.randomUUID()
                logger.info("Using Kafka consumer groupId generated from prefix: {}", groupId)
                props[ConsumerConfig.GROUP_ID_CONFIG] = groupId
            }
            kafkaProperties.groupId != null -> {
                logger.info("Using Kafka consumer fixed groupId: {}", kafkaProperties.groupId)
                props[ConsumerConfig.GROUP_ID_CONFIG] = kafkaProperties.groupId
            }
            else -> logger.info("Kafka consumer started without groupId - assign mode will be used")
        }

        kafkaProperties.groupInstanceId?.let {
            props[ConsumerConfig.GROUP_INSTANCE_ID_CONFIG] = it
        }

        val valueDeserializer = JsonDeserializer(ValidationPayload::class.java, objectMapper).apply {
            addTrustedPackages("*")
            setUseTypeMapperForKey(false)
        }

        return DefaultKafkaConsumerFactory(props, StringDeserializer(), valueDeserializer)
    }

    @Bean(name = ["validatorKafkaListenerContainerFactory"])
    fun kafkaListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, ValidationPayload>,
        kafkaProperties: ValidatorKafkaProperties
    ): ConcurrentKafkaListenerContainerFactory<String, ValidationPayload> {
        return ConcurrentKafkaListenerContainerFactory<String, ValidationPayload>().apply {
            this.consumerFactory = consumerFactory
            containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
            setConcurrency(kafkaProperties.concurrency)
            setCommonErrorHandler(DefaultErrorHandler(FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS)))
        }
    }

    private fun ValidatorKafkaProperties.buildJaasConfig(username: String, password: String): String {
        val module = when (saslMechanism) {
            "PLAIN" -> "org.apache.kafka.common.security.plain.PlainLoginModule"
            else -> "org.apache.kafka.common.security.scram.ScramLoginModule"
        }
        return "$module required username=\"${escape(username)}\" password=\"${escape(password)}\";"
    }

    private fun MutableMap<String, Any>.applySecurity(kafkaProperties: ValidatorKafkaProperties) {
        kafkaProperties.securityProtocol?.let {
            this[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = it
        }

        val requiresSasl = kafkaProperties.securityProtocol?.uppercase()?.startsWith("SASL") == true
        val credentials = kafkaProperties.resolveCredentials(requiresSasl) ?: return

        if (!requiresSasl) {
            return
        }

        val (username, password) = credentials
        this[SaslConfigs.SASL_JAAS_CONFIG] = kafkaProperties.buildJaasConfig(username, password)
        kafkaProperties.saslMechanism?.let { mechanism ->
            this[SaslConfigs.SASL_MECHANISM] = mechanism
        }
    }

    private fun ValidatorKafkaProperties.resolveCredentials(requiresSasl: Boolean): Pair<String, String>? {
        val sanitizedUsername = username?.takeUnless { it.isBlank() }
        val sanitizedPassword = password?.takeUnless { it.isBlank() }

        return when {
            sanitizedUsername == null && sanitizedPassword == null -> {
                if (requiresSasl) {
                    throw IllegalStateException(
                        "Для security.protocol=${securityProtocol} необходимо указать validator.kafka.username и validator.kafka.password"
                    )
                }
                null
            }

            sanitizedUsername == null || sanitizedPassword == null -> throw IllegalStateException(
                "Нужно одновременно задать validator.kafka.username и validator.kafka.password"
            )

            else -> sanitizedUsername to sanitizedPassword
        }
    }

    private fun escape(value: String): String =
        value.replace("\\", "\\\\").replace("\"", "\\\"")

    @Bean
    fun inputTopic(topicsProperties: ValidatorTopicsProperties): NewTopic =
        TopicBuilder.name(topicsProperties.input)
            .partitions(topicsProperties.partitions)
            .replicas(topicsProperties.replicationFactor.toInt())
            .build()

    @Bean
    fun outputTopic(topicsProperties: ValidatorTopicsProperties): NewTopic =
        TopicBuilder.name(topicsProperties.output)
            .partitions(topicsProperties.partitions)
            .replicas(topicsProperties.replicationFactor.toInt())
            .build()
}

