# Руководство по запуску сервиса Validator

Этот репозиторий содержит Spring Boot сервис (`validator-service`), который валидирует
входящие события Kafka и публикует обогащённые сообщения в выходной топик.
Ниже приведена последовательность действий для запуска сервиса вместе с
Redpanda из локального `docker-compose`.

## Предварительные требования

- Docker и Docker Compose
- JDK 21+
- Терминал с Unix-инструментами (Bash)

## 1. Запустите инфраструктуру Kafka

Из корня проекта поднимите Redpanda и Redpanda Console:

```bash
docker compose up -d
```

Compose-файл пробрасывает Kafka-листенер на `localhost:8817`, а веб-интерфейс
консоли — на `http://localhost:8055`. Брокер автоматически создаёт топик
`out_validator` и настраивает SCRAM-учётные данные:

- **Логин:** `validator_user`
- **Пароль:** `validator_pass`
- **Механизм SASL:** `SCRAM-SHA-256`

При необходимости можно посмотреть логи запуска:

```bash
docker compose logs -f redpanda
```

## 2. Экспортируйте параметры подключения Kafka

Сервис читает конфигурацию Kafka из переменных окружения. Для встроенного
стека Redpanda выполните следующие команды перед запуском приложения:

```bash
export VALIDATOR_KAFKA_BOOTSTRAP=localhost:8817
export VALIDATOR_KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
export VALIDATOR_KAFKA_SASL_MECHANISM=SCRAM-SHA-256
export VALIDATOR_KAFKA_USERNAME=validator_user
export VALIDATOR_KAFKA_PASSWORD=validator_pass
```

При необходимости можно переопределить другие свойства `validator.kafka.*`,
например параметры consumer group или степень параллелизма.

## 3. Запустите Spring Boot сервис

Используйте Gradle wrapper для старта приложения:

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

Если `type_action == 100`, сервис добавляет отметку времени и публикует
сообщение в топик `out_validator`. В остальных случаях он пишет предупреждение
в лог и завершает обработку.

## 5. Проверьте содержимое выходного топика

Откройте Redpanda Console по адресу `http://localhost:8055`, авторизуйтесь с
указанными выше учётными данными и найдите топик `out_validator`, чтобы
убедиться в публикации валидированного события.

## 6. Остановите окружение

Остановите Spring Boot сервис (Ctrl+C), затем завершите работу Docker-сервисов:

```bash
docker compose down
```

Эта команда остановит Redpanda и веб-консоль.
