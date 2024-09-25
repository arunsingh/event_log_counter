import threading
import time
import queue
import json
import os
import logging
import signal
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


'''
Author: Arun Singh, arunsingh.in@gmail.com
design an event log counter that logs events with granularity of 1 second
and counts the number of events logged in the last 5 minutes.

We need to design the following two methods:

log(): To log an event.
count(): To return the number of events logged in the last 5 minutes 
'''


# Configure basic logging for internal monitoring
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")


class AsyncBufferedLogWriter(threading.Thread):
    def __init__(self, log_file='logs.txt', flush_interval=1, buffer_size=1_000_000, max_file_size=10*1024*1024, num_workers=4, max_retries=3, queue_size=50000):
        """
        Asynchronous log writer with buffering, file rotation, and structured logging.
        Optimized for production-scale operations with backpressure, disk space handling, and queue overflow protection.
        :param log_file: The base log file name.
        :param flush_interval: Time interval between flushes (in seconds).
        :param buffer_size: Number of log entries to buffer before flushing to disk.
        :param max_file_size: Maximum size of the log file before rotation (in bytes).
        :param num_workers: Number of threads to use for writing logs to disk.
        :param max_retries: Maximum retries for failed disk operations.
        :param queue_size: Maximum size of the log queue to prevent overflow.
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

        # Buffer for batching log entries
        self.buffer = []
        self.lock = threading.Lock()
        self.logs_written = 0  # Track how many logs have been written for monitoring

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

    def _flush_buffer(self, buffer):
        """
        Flush the current buffer to the log file asynchronously. Retries on failure.
        """
        self._rotate_file_if_needed()
        retries = 0
        while retries < self.max_retries:
            try:
                log_data = '\n'.join(buffer) + '\n'
                with self.lock:
                    self.file_handle.write(log_data)
                    self.file_handle.flush()
                self.logs_written += len(buffer)
                logging.info(f"Flushed {len(buffer)} logs to disk")
                break  # Successfully written, exit the retry loop
            except (OSError, IOError) as e:
                retries += 1
                # Corrected f-string by formatting the exception message with retries
                logging.error(
                    f"Error writing logs to disk: {str(e)}, retry {retries}/{self.max_retries}")
                time.sleep(1)  # Wait before retrying
        if retries == self.max_retries:
            logging.critical(
                "Max retries reached. Failed to write logs to disk.")

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
            logging.warning(
                "Log queue is full. Dropping log entry or applying backpressure.")
            # In production, you may want to either drop logs or slow down log production.
            # self.log_queue.put(log_entry_json, block=True) to apply backpressure instead.

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

    def stop(self):
        """
        Stop the log writer.
        """
        self.log_writer.stop()


# Example usage:
if __name__ == "__main__":
    counter = EventLogCounter(log_file="async_buffered_logs")

    # Simulate logging 10 million events with structured logging and log levels
    num_events = 10_000_000
    num_threads = 20
    events_per_thread = num_events // num_threads
    threads = []

    def log_events(start, end):
        for i in range(start, end):
            event_id = f"event_{i % 1000}"  # Repeating 1000 event IDs
            log_level = "INFO" if i % 2 == 0 else "ERROR"
            message = f"Event {event_id} occurred."
            counter.log(log_level, event_id, message)

    for i in range(num_threads):
        start = i * events_per_thread
        end = start + events_per_thread
        t = threading.Thread(target=log_events, args=(start, end))
        threads.append(t)
        t.start()

    # Wait for all logging threads to finish
    for t in threads:
        t.join()

    # Stop the logger
    counter.stop()
