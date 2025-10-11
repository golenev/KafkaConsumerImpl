# Validator Service Setup Guide

This project contains a Spring Boot service (`validator-service`) that validates
incoming Kafka events and republishes the enriched payload to an output topic.
The instructions below describe how to run the service together with the
Redpanda broker defined in this repository.

## Prerequisites

- Docker & Docker Compose
- JDK 21+
- Bash shell (commands below use standard Unix tooling)

## 1. Start the Kafka infrastructure

From the project root, launch Redpanda and the Redpanda Console:

```bash
docker compose up -d
```

The compose file exposes the Kafka listener on `localhost:8817` and the console
UI on `http://localhost:8055`. The broker creates the `out_validator` topic and
provisions SCRAM credentials automatically:

- **Username:** `validator_user`
- **Password:** `validator_pass`
- **SASL mechanism:** `SCRAM-SHA-256`

You can follow the startup logs if needed:

```bash
docker compose logs -f redpanda
```

## 2. Export Kafka connection settings

The service reads its Kafka configuration from environment variables. When using
the bundled Redpanda stack, export the following before starting the
application:

```bash
export VALIDATOR_KAFKA_BOOTSTRAP=localhost:8817
export VALIDATOR_KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
export VALIDATOR_KAFKA_SASL_MECHANISM=SCRAM-SHA-256
export VALIDATOR_KAFKA_USERNAME=validator_user
export VALIDATOR_KAFKA_PASSWORD=validator_pass
```

Feel free to override any other `validator.kafka.*` property the same way if you
need to tweak consumer group behaviour or concurrency.

## 3. Run the Spring Boot service

Use the Gradle wrapper to start the Spring Boot application:

```bash
./gradlew :validator-service:bootRun
```

The service listens on `http://localhost:8085`.

### Alternative: build a runnable JAR

```bash
./gradlew :validator-service:bootJar
java -jar validator-service/build/libs/validator-service-0.0.1-SNAPSHOT.jar
```

## 4. Publish a validation request

Send a POST request to the REST endpoint, using any HTTP client. Example with
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

When `type_action == 100`, the service appends a timestamp and writes the message
to the `out_validator` topic. Otherwise it logs a validation warning and stops
processing.

## 5. Inspect the output topic

Open the Redpanda Console at `http://localhost:8055`, sign in with the
credentials above, and view the `out_validator` topic to confirm that the
validated event has been published.

## 6. Shut everything down

Stop the Spring Boot process (Ctrl+C), then tear down the Docker services:

```bash
docker compose down
```

This stops both Redpanda and the console UI.
