FROM golang:1.9-alpine

ENV KAFKA_BROKERS=kafka:9092
ENV KAFKA_TOPICS=datadog
ENV KAFKA_CONSUMER_GROUP=sandbox

WORKDIR "/go/src/app"
COPY . .

RUN go-wrapper download
RUN go-wrapper install

CMD ["go-wrapper", "run"]
