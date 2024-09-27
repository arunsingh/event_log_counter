# event_log_counter
Distributed event log counter for Production Log Environments
This program logs a high volume of events to a file in an asynchronous, buffered, and thread-safe manner. Here's a quick guide on how to set up and run the program.

Prerequisites:
Python 3.7+: Ensure Python 3.7 or above is installed on your system.
pip: Ensure you have pip installed for managing packages if required.
Steps to Run the Program:
Save the Code:

Save the provided Python code in a file, for example, distributed_event_logger.py.
Run the Script: Open a terminal and run the script using Python:

`
python3 distributed_event_logger.py

`
What the Program Does:

The program simulates logging 10 million events using multiple threads.
Events are logged asynchronously into a file, using a buffer to minimize I/O operations.
Logs are structured in JSON format and contain a timestamp, event ID, log level (INFO or ERROR), and an optional message.
The log file will be rotated when it reaches a specified size (default is 10 MB).
Files Generated:

The log files will be saved in the same directory where the script is executed, with names like async_buffered_logs_YYYYMMDD_HHMMSS.log.
These files will rotate when they exceed the maximum size (e.g., 10 MB), and a new file with a timestamp will be created.
Code Parameters:
log_file: The base file name for the log files. The actual files will be created with timestamps in the format async_buffered_logs_YYYYMMDD_HHMMSS.log.
buffer_size: The number of log entries to buffer before flushing them to the file. Default is 1,000,000 entries.
flush_interval: The time interval (in seconds) after which the buffer is flushed to the disk. Default is 1 second.
max_file_size: The maximum size (in bytes) of a log file before it rotates. Default is 10 MB.
num_workers: The number of threads used to asynchronously write logs to the file. Default is 4.
Example of Adjusting Parameters:
If you want to change the number of events to log, the size of the buffer, or any other parameter, modify the main() function as needed:

`
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

`
Output:
The script will create log files in the format async_buffered_logs_YYYYMMDD_HHMMSS.log, and they will rotate once they reach the size limit (e.g., 10 MB).

Testing:
Run the program and it should now run continuously, logging events across multiple threads and correctly flushing the logs to disk in an asynchronous, buffered manner.

Key Production-Grade Improvements done in the code:
1. Error Handling and Retries: Disk I/O errors are handled gracefully with a retry mechanism. If writing to the disk fails (e.g., due to a temporary issue), it will retry up to max_retries before giving up.
In case of critical failure (e.g., all retries failed), a critical log message is emitted.
2. Thread Management: The ThreadPoolExecutor is used to manage parallel file writing in a scalable manner. The number of workers (num_workers) can be adjusted based on the I/O capacity of the system.
The main thread joins the logger thread to ensure graceful shutdown and that all logs are written before the program exits.
3. Logging and Monitoring: Internal logging is added to track important events like log rotation, buffer flushing, and errors. This helps monitor the system’s health in production.
You can replace logging.info, logging.error, and logging.critical with more sophisticated monitoring solutions (e.g., Prometheus, Datadog) to collect metrics like the number of logs written, failure rates, etc.
4. Fault Tolerance: The system is fault-tolerant by allowing graceful handling of file I/O failures and avoiding data loss during shutdown. Even if a temporary issue occurs (e.g., disk full, transient failure), the retry mechanism allows the system to recover.
5. Memory-Efficient Buffering: The buffer size is dynamically managed, and logs are written in batches to reduce memory footprint and prevent the buffer from growing uncontrollably.
6. Scalability: The system supports horizontal scalability by allowing multiple logging instances to run concurrently.
The buffer size and flush interval are configurable and can be adjusted based on the system load.
7. File Rotation: The log files are rotated based on size (max_file_size), ensuring that log files do not become excessively large, which could degrade performance.
You can also easily extend the code to rotate logs based on time (e.g., hourly/daily).
Monitoring Metrics: You can easily extend this program to expose metrics such as:

Logs written per second: Measure throughput.
Buffer size: To monitor memory usage.
Failed writes: To detect any disk-related issues.
Retries: To understand how often retries are happening and track disk errors.
These metrics can be exposed via Prometheus, Datadog, or any other monitoring platform to track the health of the logging system in production.
