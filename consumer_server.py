import json
import random
import asyncio
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic



BROKER_URL = "PLAINTEXT://localhost:9092"


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])

    while True:
        #
        # TODO: Write a loop that uses consume to grab 5 messages at a time and has a timeout.
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.consume
        #
        messages = c.consume(5, timeout=1.0)

        # TODO: Print something to indicate how many messages you've consumed. Print the key and value of
        #       any message(s) you consumed
        print(f"consumed {len(messages)} messages")
        for message in messages:
            print(f"consume message {message.key()}: {message.value()}")

        # Do not delete this!
        await asyncio.sleep(0.01)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    try:
        asyncio.run(kafka_consumerc("com.sf.crimedata"))
    except KeyboardInterrupt as e:
        print("shutting down")

async def kafka_consumerc(topic_name):
    """Runs the Producer and Consumer tasks"""
    consumer_task = asyncio.create_task(consume(topic_name))
    await consumer_task





if __name__ == "__main__":
    main()
