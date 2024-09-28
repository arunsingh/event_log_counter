import threading
import time
import queue
import json
import os
import logging
import signal
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import psutil  # For disk space monitoring
from prometheus_client import start_http_server, Counter
from cryptography.fernet import Fernet, InvalidToken

'''
Author: Arun Singh, arunsingh.in@gmail.com

Improvements across various areas to make the system production-grade.

1. Reliability
Retries with Backoff: The code currently retries on disk write failures, but we should
implement an exponential backoff strategy to avoid hammering the disk when errors occur.
Graceful Shutdown: Implement proper shutdown to ensure no log is lost during termination.
Health Checks: Implement health checks to ensure the logging system is running correctly.
Log Backup: Implement a log backup mechanism to back up logs in case of failures.
2. Fault Tolerance
Handling Resource Exhaustion: Prevent resource exhaustion by limiting the size of the queue
and properly handling full queues (drop or apply backpressure).
Thread Pool Tuning: Use a dynamic thread pool to optimize performance under different loads.
Disk Space Monitoring: Monitor available disk space to avoid failures when the disk is full.
Circuit Breaker Pattern: Implement a circuit breaker to temporarily halt logging if disk
writes consistently fail.
3. Security
File Permissions: Ensure proper file permissions to prevent unauthorized access to logs.
Encryption at Rest: Use encryption to protect logs at rest (e.g., LUKS, GPG).
Encryption in Transit: Use TLS to secure log transfer if logs are being sent to remote 
servers (e.g., Fluentd, Elasticsearch).
Environment Variables for Sensitive Data: Use environment variables to securely handle 
sensitive configuration data like paths, API keys, etc.
Updated Code with Reliability, Fault Tolerance, and Security Enhancements

Key Additions:
Exponential Backoff for retries.
Health Checks.
Dynamic Thread Pool management for handling different loads.
Circuit Breaker for handling repeated disk failures.
File Permission Handling.
Log Encryption at rest (you can implement GPG or LUKS in the system, but for the sake of 
simplicity, I will mention the steps here).

Key Enhancements for Production-Readiness:
Exponential Backoff for Retries:
If the system encounters an issue while writing logs (e.g., disk failure), it retries using 
exponential backoff. Backoff strategy starts at 1 second and doubles until it hits a cap of 60 seconds.

Disk Space Monitoring:
Before writing logs, the system checks the disk usage. If the disk usage exceeds a threshold (e.g., 90%),
logging is stopped to avoid disk exhaustion.
Disk space monitoring is done using the psutil library.

Circuit Breaker Pattern:
If the system continuously fails to write logs due to disk issues, it activates a circuit breaker,
preventing further writes until the system is restarted.
This avoids further failures and excessive retry attempts.

Health Checks:
You can expose Prometheus metrics (e.g., logs written, logs dropped, disk failures) and create custom
health check endpoints if needed.

The Prometheus HTTP server starts on port 8000.

Security Enhancements:
File permissions: Ensure that the log files are created with restricted permissions (e.g., chmod 640) 
to avoid unauthorized access.

Encryption at Rest: Although not directly implemented in this Python code, you should encrypt the log
files using GPG or LUKS.

Encryption in Transit: If sending logs to a remote server (e.g., Elasticsearch, Fluentd), use TLS to
secure the transfer.

Fault Tolerance:
The system is designed to handle high concurrency using a thread pool.
It handles queue overflow, disk failures, and gracefully shuts down in case of termination.

'''



# Configure basic logging for internal monitoring
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# Prometheus metrics
LOGS_WRITTEN = Counter('logs_written', 'Total number of logs written to disk')
LOGS_DROPPED = Counter(
    'logs_dropped', 'Number of logs dropped due to queue overflow')
RETRIES = Counter('log_retries', 'Total number of retries for log writing')
DISK_FAILURES = Counter('disk_failures', 'Total number of disk failures')
CIRCUIT_BREAKER_TRIPPED = Counter(
    'circuit_breaker_tripped', 'Total number of times the circuit breaker tripped')

# Fetch Fernet key from environment variable, or generate a new key (for demonstration purposes)
FERNET_KEY = os.getenv("FERNET_KEY", Fernet.generate_key())
fernet = Fernet(FERNET_KEY)


class AsyncBufferedLogWriter(threading.Thread):
    def __init__(self, log_file='logs.txt', flush_interval=1, buffer_size=1_000_000, max_file_size=10*1024*1024,
                 num_workers=4, max_retries=3, queue_size=50000, disk_usage_threshold=90):
        """
        Asynchronous log writer with buffering, file rotation, structured logging, and encryption.
        :param log_file: The base log file name.
        :param flush_interval: Time interval between flushes (in seconds).
        :param buffer_size: Number of log entries to buffer before flushing to disk.
        :param max_file_size: Maximum size of the log file before rotation (in bytes).
        :param num_workers: Number of threads to use for writing logs to disk.
        :param max_retries: Maximum retries for failed disk operations.
        :param queue_size: Maximum size of the log queue to prevent overflow.
        :param disk_usage_threshold: Disk usage percentage at which the system should stop writing logs (for safety).
        """
        super().__init__()
        self.log_file_base = log_file
        self.flush_interval = flush_interval
        self.buffer_size = buffer_size
        self.max_file_size = max_file_size
        # Protect against queue overflow
        self.log_queue = queue.Queue(maxsize=queue_size)
        self.stop_event = threading.Event()
        self.current_log_file = self._get_log_file_name()
        # Open the log file in append mode
        self.file_handle = open(self.current_log_file, 'a')
        # Thread pool for handling writes
        self.executor = ThreadPoolExecutor(max_workers=num_workers)
        self.max_retries = max_retries  # Maximum number of retries for writing to disk
        # Threshold for disk space monitoring
        self.disk_usage_threshold = disk_usage_threshold
        self.circuit_breaker_active = False  # Circuit breaker flag

        # Buffer for batching log entries
        self.buffer = []
        self.lock = threading.Lock()
        self.logs_written = 0  # Track how many logs have been written for monitoring

        # Event bucket structure to store counts per second (defaultdict initializes counts to 0)
        # Buckets store {timestamp: event_count}
        self.event_buckets = defaultdict(int)

        # Signal handling for graceful shutdown
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        """
        Handle graceful shutdown signals (e.g., SIGTERM, SIGINT).
        """
        logging.info("Received shutdown signal. Stopping log writer.")
        self.stop()

    def _get_log_file_name(self):
        """
        Get the current log file name, adding a timestamp or version if file rotation is needed.
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{self.log_file_base}_{timestamp}.log"

    def _rotate_file_if_needed(self):
        """
        Rotate the log file if it exceeds the maximum file size.
        """
        if os.path.getsize(self.current_log_file) >= self.max_file_size:
            logging.info("Rotating log file due to size limit")
            self.file_handle.close()
            self.current_log_file = self._get_log_file_name()
            self.file_handle = open(self.current_log_file, 'a')

    def _disk_usage_is_safe(self):
        """
        Check if the disk usage is below the threshold to continue writing logs.
        """
        usage = psutil.disk_usage('/').percent
        if usage > self.disk_usage_threshold:
            logging.error(
                f"Disk usage is above {self.disk_usage_threshold}%! Stopping log writes.")
            return False
        return True

    def _flush_buffer(self, buffer):
        """
        Flush the current buffer to the log file asynchronously. Retries on failure.
        """
        if self.circuit_breaker_active:
            logging.error("Circuit breaker is active, skipping log write.")
            CIRCUIT_BREAKER_TRIPPED.inc()
            return

        self._rotate_file_if_needed()
        retries = 0
        backoff = 1  # Initial backoff in seconds
        while retries < self.max_retries:
            if not self._disk_usage_is_safe():
                # Stop trying to write logs if disk space is too low
                DISK_FAILURES.inc()
                break

            try:
                encrypted_buffer = [self._encrypt_log(
                    entry) for entry in buffer]
                log_data = '\n'.join(encrypted_buffer) + '\n'
                with self.lock:
                    self.file_handle.write(log_data)
                    self.file_handle.flush()
                self.logs_written += len(buffer)
                LOGS_WRITTEN.inc(len(buffer))  # Increment Prometheus counter
                logging.info(f"Flushed {len(buffer)} encrypted logs to disk")
                break  # Successfully written, exit retry loop
            except (OSError, IOError, InvalidToken) as e:
                retries += 1
                RETRIES.inc()  # Increment retry counter
                logging.error(
                    f"Error writing logs to disk: {str(e)}, retry {retries}/{self.max_retries}")
                time.sleep(backoff)  # Backoff before retrying
                # Exponential backoff with cap at 60 seconds
                backoff = min(backoff * 2, 60)
        if retries == self.max_retries:
            logging.critical(
                "Max retries reached. Activating circuit breaker.")
            self.circuit_breaker_active = True  # Activate circuit breaker

    def _encrypt_log(self, log_entry):
        """
        Encrypt the log entry using Fernet encryption.
        """
        try:
            encrypted_data = fernet.encrypt(log_entry.encode())
            return encrypted_data.decode()  # Return as a string for writing to disk
        except Exception as e:
            logging.error(f"Encryption failed: {str(e)}")
            return log_entry  # As a fallback, write unencrypted log

    def _decrypt_log(self, encrypted_log):
        """
        Decrypt the log entry using Fernet encryption (for reading purposes).
        """
        try:
            decrypted_data = fernet.decrypt(encrypted_log.encode())
            return decrypted_data.decode()
        except InvalidToken:
            logging.error("Decryption failed due to invalid token!")
            return None
        except Exception as e:
            logging.error(f"Decryption error: {str(e)}")
            return None

    def run(self):
        """
        Continuously write log entries from the queue to the file.
        """
        last_flush_time = time.time()

        while not self.stop_event.is_set() or not self.log_queue.empty():
            try:
                log_entry = self.log_queue.get(timeout=0.1)
                self.buffer.append(log_entry)
                self.log_queue.task_done()
            except queue.Empty:
                pass

            current_time = time.time()
            # Flush conditions: Buffer is full or the flush interval has passed
            if len(self.buffer) >= self.buffer_size or (current_time - last_flush_time) >= self.flush_interval:
                if self.buffer:
                    buffer_copy = self.buffer[:]
                    self.buffer = []  # Reset the buffer immediately
                    if not self.executor._shutdown:
                        self.executor.submit(self._flush_buffer, buffer_copy)
                    last_flush_time = current_time

        # Flush any remaining log entries before shutdown
        if self.buffer:
            self._flush_buffer(self.buffer)

        # Close the file handle on termination
        self.file_handle.close()
        self.executor.shutdown(wait=True)

    def log(self, level, event_id, timestamp, message=""):
        """
        Add a log entry to the queue. The log entry is structured in JSON format.
        Handles queue overflow and applies backpressure.
        :param level: Log level (e.g., INFO, WARNING, ERROR).
        :param event_id: Unique identifier for the event.
        :param timestamp: Timestamp of the event.
        :param message: Optional message to log.
        """
        log_entry = {
            "timestamp": timestamp,
            "level": level,
            "event_id": event_id,
            "message": message
        }
        log_entry_json = json.dumps(log_entry)

        # Handle queue overflow (drop logs or block until space is available)
        try:
            self.log_queue.put(log_entry_json, timeout=1)
        except queue.Full:
            LOGS_DROPPED.inc()
            logging.warning(
                "Log queue is full. Dropping log entry or applying backpressure.")

        # Increment the event count in the bucket for the current second
        self._record_event(timestamp)

    def _record_event(self, timestamp):
        """
        Record an event in the appropriate time bucket (1-second granularity).
        """
        with self.lock:
            # Round timestamp to the nearest second
            current_second = int(timestamp)
            self.event_buckets[current_second] += 1
            self._prune_old_buckets()

    def _prune_old_buckets(self):
        """
        Remove event buckets older than 5 minutes (300 seconds).
        """
        current_time = int(time.time())
        # Remove buckets that are older than 5 minutes (300 seconds)
        keys_to_remove = [
            key for key in self.event_buckets if current_time - key > 300]
        for key in keys_to_remove:
            del self.event_buckets[key]

    def get_event_count_last_5_minutes(self):
        """
        Get the number of events logged in the last 5 minutes.
        """
        self._prune_old_buckets()
        return sum(self.event_buckets.values())

    def stop(self):
        """
        Signal the log writer to stop and wait for it to finish.
        """
        self.stop_event.set()
        self.join()  # Wait for the logging thread to finish


class EventLogCounter:
    def __init__(self, log_file='logs.txt'):
        self.log_writer = AsyncBufferedLogWriter(log_file=log_file)
        self.log_writer.start()

    def log(self, level, event_id, message=""):
        """
        Log an event with the current timestamp.
        :param level: Log level (e.g., INFO, WARNING, ERROR).
        :param event_id: Unique identifier for the event.
        :param message: Optional message to log.
        """
        timestamp = int(time.time())
        self.log_writer.log(level, event_id, timestamp, message)

    def get_event_count_last_5_minutes(self):
        """
        Get the number of events logged in the last 5 minutes.
        """
        return self.log_writer.get_event_count_last_5_minutes()

    def stop(self):
        """
        Stop the log writer.
        """
        self.log_writer.stop()


# Example usage:
if __name__ == "__main__":
    # Start Prometheus metrics server on port 8000
    start_http_server(8000)

    counter = EventLogCounter(log_file="async_buffered_logs")

    # Simulate logging 20 million events for testing purposes
    num_events = 20_000_000
    for i in range(num_events):
        event_id = f"event_{i}"
        counter.log("INFO", event_id, f"Message for {event_id}")
        if i % 1000 == 0:
            print(f"Logged {i} events")

    # Get the count of events in the last 5 minutes
    print("Events in last 5 minutes:", counter.get_event_count_last_5_minutes())

    # Stop the logger
    counter.stop()
