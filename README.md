# Руководство по запуску сервиса Validator

## Структура модулей

- `:` (корневой модуль) — содержит библиотеку `consumer` с обобщённым `ConsumerKafkaService` и конфигурацией, которую переиспользуют остальные части проекта. Здесь же лежат общие зависимости и настройки Gradle для всех модулей.
- `validator-service` — Spring Boot-приложение, публикующее входящие запросы в Kafka и валидирующее события перед отправкой в исходящий топик.
- `validator-e2e-tests` — модуль интеграционных тестов, который поднимает сервис, взаимодействует с Kafka и проверяет сквозной поток данных.

## Технологии

- Kotlin 2.1 с Gradle Kotlin DSL
- Spring Boot 3.3 (Web, Validation, Spring for Apache Kafka)
- Apache Kafka Client 3.7 и Kafdrop для наблюдения за брокером
- Jackson для (де)сериализации JSON
- JUnit 5 и Kotest для автоматизированных тестов

## Запуск проекта

1. Запустите Spring-приложение, вызвав `fun main(args: Array<String>) { runApplication<ValidatorApplication>(*args) }` из `validator-service/src/main/kotlin/com/validator/app/ValidatorApplication.kt`. Это можно сделать из IDE или командой `./gradlew :validator-service:bootRun`.
2. Поднимите инфраструктуру Kafka и Kafdrop: `docker compose up -d`. Файл `docker-compose.yml` расположен в корне репозитория.
3. Откройте веб-интерфейс Kafdrop по адресу http://localhost:9000, чтобы убедиться, что брокер доступен и топики созданы.

## Проверка логики через тесты и Kafdrop

Интеграционные тесты модуля `validator-e2e-tests` поднимают сервис, публикуют события и считывают ответы из Kafka. Наблюдая за выполнением тестов и параллельно отслеживая очереди в Kafdrop, можно проследить весь жизненный цикл сообщения: от REST-запроса до появления валидированного события в выходном топике, проверить задержки и убедиться, что сервис корректно коммитит смещения.

## Как работает модуль `consumer`

1. В файле [`src/main/kotlin/consumer/Cfgs.kt`](src/main/kotlin/consumer/Cfgs.kt) создаётся экземпляр `ConsumerKafkaConfig`, который хранит настройки подключения: адреса брокеров, логин, пароль и параметры безопасности (`securityProtocol`, `saslMechanism`). Здесь же задаются поведенческие опции консюмера — отключается автокоммит, выбирается топик (`awaitTopic`), объект `ObjectMapper` для разбора сообщений и класс, в который нужно десериализовать события. При необходимости можно оставить `groupId` и `groupIdPrefix` равными `null`, чтобы читать сообщения напрямую из партиций без участия группы.

   ```kotlin
   val someConsumerConfig = ConsumerKafkaConfig(
       bootstrapServers = "...",
       username = "demo",
       password = "demo",
   ).apply {
       securityProtocol = "SASL_PLAINTEXT"
       saslMechanism = "SCRAM-SHA-256"
       groupId = null
       groupIdPrefix = null
       autoCommit = false
       awaitTopic = "some_topic"
       awaitMapper = ObjectMapper()
       awaitClazz = MyEvent::class.java
   }
   ```

2. При подготовке теста (например, в блоке `@BeforeEach`) фабричный метод `runService` создаёт `ConsumerKafkaService` в отдельном потоке. Сервис реализует `Runnable`: метод `start()` инициализирует `KafkaConsumer`, настраивает сериализаторы ключа/значения и поднимает поток с именем `kafka-await-<topic>`, который будет выполнять метод `run()` и обрабатывать сообщения.

   ```kotlin
   private lateinit var consumerService: ConsumerKafkaService<MyEvent>

   @BeforeEach
   fun setUp() {
       consumerService = ConsumerKafkaService(
           cfg = someConsumerConfig,
           clazz = MyEvent::class.java,
           keySelector = { it.id }
       )
       consumerService.start()
   }
   ```
3. Если `groupId = null` и `groupIdPrefix = null`, метод `run()` работает в режиме прямого назначения партиций: запрашивает список доступных партиций через `partitionsFor`, формирует набор `TopicPartition` и вызывает `assign`. Затем `positionToTailOrLastN` вычисляет смещения «конца» каждой партиции и отматывает консюмер либо на самый конец (если `awaitLastNPerPartition <= 0`), либо на последние N сообщений из конфигурации, чтобы перечитать только свежие данные.

### `subscribe` против `assign`

- **Когда задан `groupId` или `groupIdPrefix`.** Ветка `if (hasGroupId())` в `run()` вызывает `consumer.subscribe(listOf(topic))`. Kafka сама распределяет партиции между участниками consumer group — отдельными экземплярами приложения, подключёнными с тем же идентификатором группы. Она реагирует на ребалансы — процесс перераспределения партиций между участниками, который инициирует координатор группы (специальный брокер, отслеживающий состояние потребителей) при изменении состава клиентов, — и требует вызова `waitForAssignment()`, чтобы дождаться фактического назначения партиций. После этого `positionToTailOrLastN` переводит смещения в нужное положение. Такой режим позволяет делить нагрузку между несколькими консюмерами и требует явного коммита смещений через `commitSync()`.

  ```kotlin
  if (hasGroupId()) {
      consumer.subscribe(listOf(topic))
      waitForAssignment()
      positionToTailOrLastN(consumer.assignment().toList())
  }
  ```

- **Когда `groupId` и `groupIdPrefix` равны `null`.** В противном случае выполнение переходит в ветку `else`, где используется `consumer.assign(tps)`. Сервис вручную собирает список `TopicPartition` через `partitionsFor` и закрепляет их за собой без участия координатора групп — того самого брокера, который наблюдает за consumer group и отвечает за запуск ребалансов. Такой подход исключает автоматические ребалансы, консюмер самостоятельно управляет позиционированием и не коммитит смещения — это удобно для тестов, которым нужно читать все сообщения из конкретных партиций в изоляции.

  ```kotlin
  else {
      val infos = consumer.partitionsFor(topic) ?: emptyList()
      val tps = infos.map { TopicPartition(topic, it.partition()) }
      consumer.assign(tps)
      positionToTailOrLastN(tps)
  }
  ```
4. Основной цикл `poll` получает батчи сообщений, пытается десериализовать JSON в тип `T` с помощью переданного `ObjectMapper` и извлекает ключ через лямбду `keySelector`. Для каждого успешно десериализованного сообщения вызывается `waiter.provide`, который складывает объект в очередь ожидания по соответствующему ключу. В режиме с `groupId` консюмер дополнительно фиксирует смещения через `commitSync`, но при `groupId = null` этот шаг пропускается.

   ```kotlin
   while (isRunningNow.get()) {
       val records = consumer.poll(Duration.ofMillis(300))
       records.forEach { record ->
           val value = mapper.readValue(record.value(), clazz)
           keySelector(value)?.let { waiter.provide(it, value) }
       }
       if (hasGroupId()) {
           consumer.commitSync()
       }
   }
   ```
5. Тесты получают сообщения через методы `waitForKeyList` и `waitForKeyListAbsent`. Первый делегирует в `WaitForMany.waitMany`, который блокирующе ждёт появления нужного количества элементов в очереди и возвращает их, либо завершает ожидание по таймауту. Второй вызывает `WaitForMany.waitNone`, убеждаясь, что за время ожидания очередь для ключа осталась пустой; в противном случае выбрасывается исключение. Под капотом `waitNone` переиспользует `waitMany` с параметрами `min = 0` и `max = 1`, а тот опирается на `LinkedBlockingQueue`, чтобы безопасно обмениваться данными между потоками: эта структура одновременно потокобезопасна, поддерживает блокирующие методы `put`/`take` с таймаутами и не требует дополнительной синхронизации, поэтому отлично подходит для координации фонового потребителя и тестового потока.

   ```kotlin
   val received: List<MyEvent> = consumerService.waitForKeyList("user-42", timeoutMs = 5_000)
   val nothing: List<MyEvent> = consumerService.waitForKeyListAbsent("user-404", timeoutMs = 1_000)
   ```
6. При остановке теста `close()` вызывает приватный `stop()`: сервис помечает себя как неактивный, будит `KafkaConsumer` через `wakeup()` и дожидается завершения фонового потока. Благодаря этому консюмер корректно закрывает соединения с брокером в блоке `finally`, что предотвращает утечки ресурсов между тестовыми сценариями.

