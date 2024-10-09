"""Reddit Kafka consumer."""

import json
import logging
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%y/%m/%d %H:%M:%S"
)
logger = logging.getLogger("Reddit Kafka Consumer")


def create_kafka_consumer(topic: str, bootstrap_servers: list[str], group_id: str) -> KafkaConsumer:
    """Create and return a Kafka consumer instance."""
    logger.info(f"Creating Kafka consumer for topic: {topic}")

    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


def process_comment(data: dict[str, str]) -> None:
    """Process and log a comment message."""
    logger.info(f"Received comment for post {data['id']}: {data['comment']}")


def process_post(data: dict[str, str]) -> None:
    """Process and log a post message."""
    logger.info(f"Received post: ID={data['id']}, Title={data['title']}, Upvote={data['upvote']}")


def process_message(message: ConsumerRecord) -> None:
    """Process a message from Kafka, determining if it's a comment or a post."""
    data = message.value
    if "comment" in data:
        process_comment(data)
    else:
        process_post(data)


def main() -> None:
    """Main function to set up the Kafka consumer and process messages."""
    bootstrap_servers = ["localhost:9092"]
    group_id = "reddit-posts-group"
    topic = "reddit-posts"

    consumer = create_kafka_consumer(topic, bootstrap_servers, group_id)

    logger.info("Consumer started. Waiting for messages...")
    for message in consumer:
        process_message(message)


if __name__ == "__main__":
    main()
