FROM alpine:3.4

ENV KAFKA_BROKERS=kafka:9092
ENV KAFKA_TOPICS=datadog
ENV KAFKA_CONSUMER_GROUP=sandbox

COPY kafka-consumer /app/kafka-consumer
COPY _static /app/_static

WORKDIR "/app"
ENTRYPOINT ["/app/kafka-consumer"]
