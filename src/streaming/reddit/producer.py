"""Reddit Kafka producer."""

import json
import logging
import os

from kafka import KafkaProducer
from praw import Reddit
from praw.models import Submission, Subreddit

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%y/%m/%d %H:%M:%S"
)
logger = logging.getLogger("Reddit Kafka Producer")


def create_reddit_client(client_id: str, client_secret: str, user_agent: str) -> Reddit:
    """Create and return a Reddit client."""
    logger.info("Creating Reddit client...")

    return Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)


def create_kafka_producer(bootstrap_servers: list[str]) -> KafkaProducer:
    """Create and return a Kafka producer."""
    logger.info("Creating Kafka producer...")

    return KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode("utf-8"))


def fetch_subreddit_posts(reddit_client: Reddit, subreddit_name: str, limit: int) -> list[Submission]:
    """Fetch the latest posts from a subreddit."""
    logger.info(f"Fetching {limit} posts from r/{subreddit_name}...")
    subreddit: Subreddit = reddit_client.subreddit(subreddit_name)

    return list(subreddit.new(limit=limit))


def create_post_data(post: Submission) -> dict[str, object]:
    """Create a dictionary with post data."""
    logger.debug(f"Creating post data for post {post.id}...")

    return {"id": post.id, "title": post.title, "upvote": post.score}


def create_comment_data(post: Submission) -> dict[str, str]:
    """Create a dictionary with comment data."""
    logger.debug(f"Creating comment data for post {post.id}...")

    return {"id": post.id, "comment": f"Interesting post about {post.title}"}


def send_kafka_message(producer: KafkaProducer, topic: str, data: dict[str, object]) -> None:
    """Send a message to Kafka."""
    logger.debug(f"Sending message to Kafka topic {topic}...")
    producer.send(topic, data)


def main() -> None:
    logger.info("Starting Reddit to Kafka producer script.")

    # Configuration
    reddit_config: dict[str, str] = {
        "user_agent": "Kafka test by Szaleju.",
        "client_id": os.getenv("REDDIT_CLIENT_ID"),
        "client_secret": os.getenv("REDDIT_CLIENT_SECRET"),
    }
    kafka_config: dict[str, list[str]] = {"bootstrap_servers": ["localhost:9092"]}
    subreddit_name: str = "programming"
    topic_name: str = "reddit-posts"
    post_limit: int = 10

    # Create clients
    reddit_client: Reddit = create_reddit_client(**reddit_config)
    kafka_producer: KafkaProducer = create_kafka_producer(**kafka_config)

    # Fetch and process posts
    posts: list[Submission] = fetch_subreddit_posts(reddit_client, subreddit_name, post_limit)
    logger.info(f"Processing {len(posts)} posts")
    for post in posts:
        post_data: dict[str, object] = create_post_data(post)
        send_kafka_message(kafka_producer, topic_name, post_data)

        comment_data: dict[str, str] = create_comment_data(post)
        send_kafka_message(kafka_producer, topic_name, comment_data)

    # Ensure all messages are sent
    kafka_producer.flush()
    logger.info("Producer finished sending messages.")


if __name__ == "__main__":
    main()
