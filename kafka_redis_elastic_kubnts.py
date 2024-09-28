import os
import json
import time
import logging
import multiprocessing
from datetime import datetime
from confluent_kafka import Producer, Consumer, KafkaException
from elasticsearch import Elasticsearch, helpers
from prometheus_client import start_http_server, Counter
from redis import Redis


'''
Author: Arun Singh, arunsingh.in@gmail.com

an architecture-ready implementation that handles log ingestion, processing, and
querying for large-scale distributed systems:

Prerequisites:
- Kafka cluster with multiple partitions and replication.
- Redis cluster for distributed in-memory storage.
- Elasticsearch cluster for log indexing.
- Kubernetes for container orchestration and auto-scaling.

Key Design Choices and Trade-offs:
- Kafka for Distributed Messaging: Kafka provides high throughput, partitioning, and replication to
handle billions of events. With sufficient partitions and replication, Kafka scales well to
hyperscale environments.

- Elasticsearch for Log Storage:

Elasticsearch provides sharded indexing and distributed searching capabilities, which is critical
when storing and querying billions of log events. Sharding ensures logs are distributed across multiple nodes.

- Redis for Event Counting: Redis is used for real-time in-memory event counting, allowing for fast 
lookups of the event count over the last 5 minutes. Pruning old events helps manage memory usage.

- Horizontal Scalability with Kubernetes:Kafka, Redis, Elasticsearch, producers, and consumers should 
be deployed on Kubernetes with auto-scaling enabled, allowing the system to grow and shrink dynamically 
based on demand.

- Fault Tolerance with Replication: Kafka topics are replicated, ensuring that event logs are safe even 
if a broker goes down. Elasticsearch uses replication to ensure log data is redundant across nodes.

This architecture helped us to handle 10 billion events at hyperscale in cloud or on-premise environments. 
It distributes the load across multiple Kafka brokers, consumers, and Elasticsearch nodes, while Redis
provides real-time event counting. Kubernetes ensures the system can dynamically scale to meet demand.
'''

# Configure basic logging for internal monitoring
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# Prometheus metrics
LOGS_WRITTEN = Counter(
    'logs_written', 'Total number of logs written to Elasticsearch')
KAFKA_ERRORS = Counter('kafka_errors', 'Number of Kafka errors')
EVENT_COUNT = Counter('event_count', 'Total number of events processed')

# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "log-events"
NUM_PARTITIONS = 100  # Number of Kafka partitions

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
redis_client = Redis(host=REDIS_HOST, port=6379, db=0)

# Elasticsearch configuration
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# Fernet encryption key management
FERNET_KEY = os.getenv("FERNET_KEY", Fernet.generate_key())
fernet = Fernet(FERNET_KEY)


class KafkaLogProducer:
    def __init__(self):
        """
        Kafka Producer to send log events to Kafka.
        """
        self.producer_config = {'bootstrap.servers': KAFKA_BROKER}
        self.producer = Producer(self.producer_config)

    def produce_log_event(self, event_data):
        """
        Produce a log event to Kafka.
        """
        try:
            self.producer.produce(KAFKA_TOPIC, value=event_data)
            self.producer.poll(0)  # Ensure delivery
        except KafkaException as e:
            KAFKA_ERRORS.inc()
            logging.error(f"Kafka error: {str(e)}")


class KafkaLogConsumer(multiprocessing.Process):
    def __init__(self, process_id):
        """
        Kafka Consumer to consume log events and send to Elasticsearch.
        """
        super().__init__()
        self.process_id = process_id
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'log-consumers',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([KAFKA_TOPIC])

    def run(self):
        """
        Run Kafka consumer and process log events.
        """
        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                KAFKA_ERRORS.inc()
                logging.error(f"Consumer error: {msg.error()}")
                continue
            event_data = msg.value().decode('utf-8')
            self.process_log_event(event_data)

    def process_log_event(self, event_data):
        """
        Process log event and send to Elasticsearch.
        """
        try:
            log_event = json.loads(event_data)
            timestamp = log_event["timestamp"]
            event_id = log_event["event_id"]

            # Index event in Elasticsearch
            doc = {
                '_index': 'logs',
                '_type': '_doc',
                '_source': {
                    'event_id': event_id,
                    'timestamp': timestamp
                }
            }
            helpers.bulk(es, [doc])  # Bulk insert into Elasticsearch
            LOGS_WRITTEN.inc()
            EVENT_COUNT.inc()

        except Exception as e:
            logging.error(f"Error processing log event: {str(e)}")


class EventLogCounter:
    def __init__(self, window_size=300):
        """
        Redis-based log counter to store events and count them over the last 5 minutes.
        :param window_size: Time window for counting events (5 minutes = 300 seconds).
        """
        self.window_size = window_size

    def log_event(self, event_id, timestamp):
        """
        Log an event with its timestamp.
        """
        current_second = int(timestamp)
        redis_client.incr(f"event_count:{current_second}")
        EVENT_COUNT.inc()

    def get_event_count_last_5_minutes(self):
        """
        Get the total number of events in the last 5 minutes.
        """
        current_time = int(time.time())
        event_count = 0
        for second in range(current_time - self.window_size, current_time):
            count = redis_client.get(f"event_count:{second}")
            if count:
                event_count += int(count)
        return event_count

    def prune_old_events(self):
        """
        Remove events older than 5 minutes from Redis.
        """
        current_time = int(time.time())
        for second in range(current_time - self.window_size - 10, current_time - self.window_size):
            redis_client.delete(f"event_count:{second}")


# Producer and Consumer processes
if __name__ == "__main__":
    # Start Prometheus metrics server
    start_http_server(8000)

    # Create multiple Kafka consumers for parallel processing
    num_consumers = 10  # Scale this for high throughput
    consumers = [KafkaLogConsumer(process_id=i) for i in range(num_consumers)]
    for consumer in consumers:
        consumer.start()

    # Simulate log event production
    producer = KafkaLogProducer()
    for i in range(10000000000):  # Simulate 10 billion events
        timestamp = int(time.time())
        event_id = f"event_{i}"
        log_event = json.dumps({"event_id": event_id, "timestamp": timestamp})
        producer.produce_log_event(log_event)
        if i % 100000 == 0:
            logging.info(f"Produced {i} events")

    # Event counting logic
    counter = EventLogCounter()
    while True:
        time.sleep(5)  # Every 5 seconds
        event_count = counter.get_event_count_last_5_minutes()
        logging.info(f"Events in last 5 minutes: {event_count}")
        counter.prune_old_events()
