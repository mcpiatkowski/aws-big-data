# Basic capabilities of message broker systems #

Write a program that creates a message producer and consumer using Apache Kafka. 
The producer program will send a sample message from [Reddit](https://www.reddit.com/r/programming/) to a Kafka topic using [Reddit API](https://www.reddit.com/dev/api/) and [Python Reddit API Wrapper](https://praw.readthedocs.io/en/stable/index.html).
The consumer program will retrieve and display the message from the same topic.

It is necessary to:

- Publish to the topic the latest ten posts from the [r/programming subreddit](https://www.reddit.com/r/programming/) using the following JSON format: {"id": post.id, "title": post.title, "upvote": post.score}.
- Publish your comment using the following JSON format: {"id": post.id, "comment": post.comment} for each published post on the topic.
- Receive all messages with your comments and display all of them.

**Acceptance criteria**:

- The producer program is connected to the Kafka broker cluster and sends a message to the specified topic.
- The consumer program is subscribed to the same topic and retrieves messages from it.
- The producer program sends messages with the content to the topic.
- The consumer program receives the message and displays it on the console.

# Kafka Setup #

Installing kafka via homebrew

`brew install kafka`

To start kafka now and restart at login:

`brew services start zookeeper`

`brew services start kafka`

Or, if you don't want/need a background service you can just run:

`/opt/homebrew/opt/kafka/bin/kafka-server-start /opt/homebrew/etc/kafka/server.properties`

Create test topic:

`kafka-topics --create --topic test-topic --bootstrap-server localhost:9092`

`kafka-topics --describe --topic test-topic --bootstrap-server localhost:9092`

Produce messages:

`kafka-console-producer --topic test-topic --bootstrap-server localhost:9092`

Consume messages:

`kafka-console-consumer --topic test-topic --from-beginning --bootstrap-server localhost:9092`

Delete topic:

`kafka-topics --delete --topic test-topic --bootstrap-server localhost:9092`

List topics:

`kafka-topics --list --bootstrap-server localhost:9092`