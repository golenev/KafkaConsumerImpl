import com.consumer.KafkaConsumerConfig
import com.consumer.KafkaMessageFinder
import com.model.ExampleKafkaRs
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import java.time.Duration

class ConsumerTest {

    private val BOOTSTRAP = ""
    private val USERNAME = ""
    private val PASSWORD = ""
    private val TOPIC = ""

    @Test
    fun `should find message by key`() {
        val cfg = KafkaConsumerConfig(BOOTSTRAP, USERNAME, PASSWORD).apply {
            // если groupId не задать → assign+seek, можно перечитывать сообщения
            groupId = null
            autoCommit = false
            autoOffsetReset = "earliest"
            securityProtocol = "SASL_PLAINTEXT" // или "SASL_SSL"
            saslMechanism = "SCRAM-SHA-256"     // setter сам выставит ScramLoginModule
        }

        val finder = KafkaMessageFinder(cfg)

        val result: ExampleKafkaRs? = finder.findByKey<ExampleKafkaRs>(
            topic = TOPIC,
            key = 918630978.toString(),
            fromBeginning = true,
            timeout = Duration.ofSeconds(20)
        )

        assertNotNull(result, "Сообщение с ключом '$' не найдено")
        println("Найдено сообщение: $result")
    }

}