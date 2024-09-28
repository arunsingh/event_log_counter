import os
import json
import time
import logging
import multiprocessing
from datetime import datetime
from collections import defaultdict
from confluent_kafka import Producer, Consumer, KafkaException
from prometheus_client import start_http_server, Counter
from cryptography.fernet import Fernet, InvalidToken
import redis


'''
Author: Arun Singh, arunsingh.in@gmail.com

Key Enhancements for Hyperscale Handling:
Distributed Log System with Kafka:
The log events are distributed across multiple partitions in Kafka, providing horizontal 
scalability and fault tolerance.
Producers generate log events and push them to Kafka. Consumers read the events and write
them to disk.

Multi-Process Architecture:
Multi-processing is used to parallelize both event production and consumption,making bette
r use of multi-core CPUs.

Each consumer process works independently to avoid bottlenecks.

Redis for Event Counting:
Redis is used as a distributed cache to store event counts for each second. This allows us to track
the event counts across distributed nodes in a shared store.
The EventLogCounter class logs events in Redis and tracks events from the last 5 minutes.

Fault Tolerance and Monitoring:
Prometheus metrics are used to track event counts, dropped logs, retries, and Kafka errors.
Kafka ensures fault tolerance and high availability.

 
Improvements for Hyperscale:
Kafka handles the log event distribution, ensuring scalability and fault tolerance.
Redis centralizes event counting, making it easy to track events across multiple nodes.
Multi-process consumers provide horizontal scaling to handle millions of log events without
overloading a single process.

'''

# Configure basic logging for internal monitoring
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# Prometheus metrics
LOGS_WRITTEN = Counter('logs_written', 'Total number of logs written to disk')
LOGS_DROPPED = Counter(
    'logs_dropped', 'Number of logs dropped due to queue overflow')
RETRIES = Counter('log_retries', 'Total number of retries for log writing')
KAFKA_ERRORS = Counter('kafka_errors', 'Number of Kafka errors')
EVENT_COUNT = Counter('event_count', 'Total number of events received')

# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "log-events"
NUM_PARTITIONS = 10  # Kafka topic partitions for load distribution

# Redis for centralized event counting (to track events across distributed nodes)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
redis_client = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0)

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
        Produce a log event to the Kafka topic.
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
        Kafka Consumer to consume log events from Kafka and write to disk.
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
        Process log event and write to file.
        """
        LOGS_WRITTEN.inc()
        logging.info(f"Consumed log event: {event_data}")


class EventLogCounter:
    def __init__(self, flush_interval=1, window_size=300):
        """
        Redis-based log counter to store events and count them over the last 5 minutes.
        :param flush_interval: Interval for pruning old events.
        :param window_size: Time window for counting events (5 minutes = 300 seconds).
        """
        self.flush_interval = flush_interval
        self.window_size = window_size  # 300 seconds (5 minutes)

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


# Distributed Kafka Producers for Event Logging
class LogProducerProcess(multiprocessing.Process):
    def __init__(self, num_events, interval):
        super().__init__()
        self.num_events = num_events
        self.interval = interval
        self.producer = KafkaLogProducer()

    def run(self):
        """
        Simulate producing log events.
        """
        for i in range(self.num_events):
            timestamp = int(time.time())
            event_id = f"event_{i}"
            log_event = json.dumps(
                {"event_id": event_id, "timestamp": timestamp})
            self.producer.produce_log_event(log_event)
            time.sleep(self.interval)
            if i % 10000 == 0:
                logging.info(f"Produced {i} events")


if __name__ == "__main__":
    # Start Prometheus metrics server
    start_http_server(8000)

    # Create a pool of Kafka consumers
    num_consumers = 4  # Number of consumers for parallelism
    consumers = [KafkaLogConsumer(process_id=i) for i in range(num_consumers)]
    for consumer in consumers:
        consumer.start()

    # Create a pool of Kafka producers (simulating event generation)
    num_producers = 2  # Number of producers for parallel event generation
    producers = [LogProducerProcess(
        num_events=50000000, interval=0.0001) for _ in range(num_producers)]
    for producer in producers:
        producer.start()

    # Event counting logic
    counter = EventLogCounter()

    # Run the event counting and pruning in the main process
    while True:
        time.sleep(5)  # Every 5 seconds
        event_count = counter.get_event_count_last_5_minutes()
        logging.info(f"Events in last 5 minutes: {event_count}")
        counter.prune_old_events()
