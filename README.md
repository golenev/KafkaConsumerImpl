# Руководство по запуску сервиса Validator

Этот репозиторий содержит Spring Boot сервис (`validator-service`), который валидирует
входящие события Kafka и публикует обогащённые сообщения в выходной топик.
Ниже описаны шаги по подготовке инфраструктуры и запуску приложения.

## Предварительные требования

- Kafka-брокер с доступом к админским утилитам (`kafka-topics.sh`, `kafka-console-consumer.sh` и т.п.)
- JDK 21+
- Gradle Wrapper (поставляется в репозитории)
- Терминал с Unix-инструментами (Bash)

## 1. Подготовьте инфраструктуру Kafka

Убедитесь, что Kafka-брокер запущен и доступен. Создайте входной и выходной топики
с несколькими партициями (например, по 3) и фактором репликации, подходящим для
вашего окружения:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic in_validator --partitions 3 --replication-factor 1

kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic out_validator --partitions 3 --replication-factor 1
```

Если требуется аутентификация, заранее подготовьте пользователя/пароль или другой
механизм безопасности и убедитесь, что он имеет права на оба топика.

## 2. Экспортируйте параметры подключения Kafka

Сервис читает конфигурацию Kafka из переменных окружения. Ниже приведён пример для
локального брокера без SASL:

```bash
export VALIDATOR_KAFKA_BOOTSTRAP=localhost:9092
export VALIDATOR_KAFKA_SECURITY_PROTOCOL=PLAINTEXT
export VALIDATOR_KAFKA_SASL_MECHANISM=
export VALIDATOR_KAFKA_USERNAME=
export VALIDATOR_KAFKA_PASSWORD=
```

При необходимости можно переопределить другие свойства `validator.kafka.*`,
например параметры consumer group или степень параллелизма.

## 3. Запустите Spring Boot сервис

Используйте Gradle Wrapper для старта приложения:

```bash
./gradlew :validator-service:bootRun
```

Сервис принимает HTTP-запросы на `http://localhost:8085`.

### Альтернатива: запуск из JAR

```bash
./gradlew :validator-service:bootJar
java -jar validator-service/build/libs/validator-service-0.0.1-SNAPSHOT.jar
```

## 4. Отправьте запрос на валидацию

Отправьте POST-запрос на REST-эндпоинт с помощью любого HTTP-клиента. Пример с
`curl`:

```bash
curl -X POST http://localhost:8085/api/v1/validation \
  -H 'Content-Type: application/json' \
  -d '{
        "eventId": "evt-123",
        "userId": "42",
        "type_action": 100,
        "status": "PENDING",
        "sourceSystem": "demo",
        "priority": 5,
        "amount": 123.45
      }'
```

Если `type_action == 100`, сервис добавляет отметку времени и публикует сообщение
в топик `out_validator`. В остальных случаях он пишет предупреждение в лог и
завершает обработку.

## 5. Проверьте содержимое выходного топика

Для проверки результата можно воспользоваться стандартными утилитами Kafka.
Например, чтобы прочитать одно сообщение из `out_validator`:

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic out_validator --from-beginning --max-messages 1
```

Либо используйте любой другой инструмент наблюдения за Kafka, доступный в вашем
окружении.

## 6. Остановите сервис

Остановите Spring Boot приложение сочетанием `Ctrl+C` (если оно запущено через
`bootRun`) либо завершите процесс JAR. После этого при необходимости отключите
Kafka-брокер штатными средствами.
