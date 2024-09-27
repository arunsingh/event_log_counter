import threading
import time
from collections import defaultdict


'''
Author: Arun Singh, arunsingh.in@gmail.com

For : dev, staging environment , NOT TO BE USED FOR PROD ENVs


PersistentLogCounter Class:

This class coordinates multiple LogShard instances (shards) to log and count events.
Each shard writes log events to the same file (log_file).
The log(event_id) method sends the event to a specific shard for logging and counting.
LogShard Class:

Manages the logging and counting of events in a specific shard.
Circular Buffer: Used to store event counts for the last 5 minutes (300 seconds).

Persistent Logging: Each log entry is written to a file (e.g., logs.txt) with a timestamp and event ID.
Logging to Persistent Storage:

The _write_log_to_file method appends log entries to the file logs.txt in the format [timestamp] event_id.
Each entry is written to the file as the event is logged.

Thread Safety: 
We use a lock (self.lock) to ensure that both logging and file writing are thread-safe. Multiple threads 
can log events concurrently without corrupting the file or counters.

Efficient File I/O:
Log entries are appended to the file. Since we are opening and closing the file for each write operation,
this ensures that the data is written to disk immediately. If desired, you could use a buffered write strategy 
(writing in batches) to improve performance for high-frequency logs.


'''

class PersistentLogCounter:
    def __init__(self, log_file='logs.txt', num_shards=10):
        """
        Initialize the log counter with a specified number of shards.
        Each shard will store event counts in a circular buffer.
        Log entries will also be stored persistently in a file.
        """
        self.num_shards = num_shards
        self.shards = [LogShard(log_file) for _ in range(num_shards)]
        self.log_file = log_file

    def _get_shard_index(self, event_id):
        """
        Simple hash function to assign an event to a shard based on event_id.
        """
        return hash(event_id) % self.num_shards

    def log(self, event_id):
        """
        Log an event by sending it to one of the shards.
        """
        shard_index = self._get_shard_index(event_id)
        self.shards[shard_index].log(event_id)

    def count(self):
        """
        Return the total number of events across all shards in the last 5 minutes.
        """
        total_count = 0
        for shard in self.shards:
            total_count += shard.count()
        return total_count


class LogShard:
    def __init__(self, log_file, window_size=300):
        """
        Each shard maintains a circular buffer with counts for each second in the last 5 minutes.
        Logs are written persistently to the specified log file.
        """
        self.window_size = window_size
        self.lock = threading.Lock()
        # Circular buffer for storing counts
        self.event_buckets = [0] * window_size
        # Timestamps for each second in the buffer
        self.timestamps = [0] * window_size
        # Pointer to the current position in the circular buffer
        self.current_index = 0
        self.log_file = log_file                # File to store logs persistently

    def _get_current_second(self):
        """
        Get the current time in seconds (epoch).
        """
        return int(time.time())

    def log(self, event_id):
        """
        Log an event in this shard.
        Write the log event to the persistent storage (file) with a timestamp.
        """
        with self.lock:
            current_time = self._get_current_second()
            index = current_time % self.window_size  # Circular index

            if self.timestamps[index] == current_time:
                # Same second, increment count
                self.event_buckets[index] += 1
            else:
                # New second, reset the bucket for this new second
                self.timestamps[index] = current_time
                self.event_buckets[index] = 1

            # Write the event to the log file with a timestamp
            self._write_log_to_file(event_id, current_time)

    def _write_log_to_file(self, event_id, timestamp):
        """
        Write the event log to the file with the format:
        [timestamp] event_id
        """
        with open(self.log_file, 'a') as f:
            f.write(f"{timestamp} {event_id}\n")

    def count(self):
        """
        Return the total number of events in this shard in the last 5 minutes.
        """
        current_time = self._get_current_second()
        total_events = 0
        with self.lock:
            for i in range(self.window_size):
                if current_time - self.timestamps[i] < self.window_size:
                    total_events += self.event_buckets[i]
        return total_events


# Example usage:
if __name__ == "__main__":
    counter = PersistentLogCounter(log_file="persistent_logs.txt")

    # Simulate logging events
    for i in range(100000):
        counter.log(f"event_{i % 1000}")  # Repeating 1000 event IDs

    # Count the events (aggregated across all shards)
    print("Total event count:", counter.count())
