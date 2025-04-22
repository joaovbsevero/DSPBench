import queue
import threading
import time

# Settings
FAST_THREADS = 12  # number of fast-task workers
SLOW_THREADS = 4  # number of slow-task workers
THRESHOLD = 1.5

# Queues for inter-stage communication
fast_queue = queue.Queue()
slow_queue = queue.Queue()
reducer_queue = queue.Queue()

# Event to signal no more incoming tasks
finish_event = threading.Event()

# Stats for demonstration
fast_counts = [0] * FAST_THREADS
slow_counts = [0] * SLOW_THREADS


def parser_generator(events: list[str]):
    """Parse -> Generate -> classify by delay -> enqueue to fast or slow queue."""
    for raw in events:
        event_id, durations = raw.split(",")
        for duration in map(int, durations.split()):
            if duration <= THRESHOLD:
                fast_queue.put((event_id, duration))
            else:
                slow_queue.put((event_id, duration))

    # Signal that parsing is done
    finish_event.set()


def fast_worker(i: int):
    """Fast worker attempts to get from its own queue or steal from slow otherwise."""
    while True:
        try:
            item = fast_queue.get_nowait()
        except queue.Empty:
            try:
                item = slow_queue.get_nowait()
            except queue.Empty:
                if finish_event.is_set() and fast_queue.empty() and slow_queue.empty():
                    break
                continue

        # Process item
        time.sleep(item[1] / 1000)
        fast_counts[i] += 1
        reducer_queue.put(item)

    # Signal reducer that this worker is done
    reducer_queue.put(None)


def slow_worker(i: int):
    """Slow worker attempts to get from its own queue or steal from fast otherwise."""
    while True:
        try:
            item = slow_queue.get_nowait()
        except queue.Empty:
            try:
                item = fast_queue.get_nowait()
            except queue.Empty:
                if finish_event.is_set() and fast_queue.empty() and slow_queue.empty():
                    break
                continue

        # Process item
        time.sleep(item[1] / 1000)
        slow_counts[i] += 1
        reducer_queue.put(item)

    # Signal reducer that this worker is done
    reducer_queue.put(None)


def reducer_worker():
    """Single-threaded keyed reduce."""
    end_signals = 0
    results = {}
    total_workers = FAST_THREADS + SLOW_THREADS
    while end_signals < total_workers:
        item = reducer_queue.get()
        if item is None:
            end_signals += 1
        else:
            event_id = item[0]
            results[event_id] = results.get(event_id, 0) + 1


def main():
    start = time.time()

    # Load events
    with open("../dspbench-flink/data/highvariance.csv") as f:
        events = f.readlines()

    # Start fast-worker threads
    fast_threads: list[threading.Thread] = []
    for i in range(FAST_THREADS):
        t = threading.Thread(target=fast_worker, args=(i,))
        t.start()
        fast_threads.append(t)

    # Start slow-worker threads
    slow_threads: list[threading.Thread] = []
    for i in range(SLOW_THREADS):
        t = threading.Thread(target=slow_worker, args=(i,))
        t.start()
        slow_threads.append(t)

    # Start reducer thread
    reducer = threading.Thread(target=reducer_worker)
    reducer.start()

    # Run parser + generator + classifier in main thread
    parser_generator(events)

    # Wait for all worker threads to finish
    for t in fast_threads + slow_threads:
        t.join()

    # Wait for reducer to finish
    reducer.join()

    # Show workload distribution
    print("Fast-worker workload distribution:")
    for i, cnt in enumerate(fast_counts):
        print(f"  fast_worker_{i}: {cnt} events")

    print("Slow-worker workload distribution:")
    for i, cnt in enumerate(slow_counts):
        print(f"  slow_worker_{i}: {cnt} events")

    end = time.time()
    print(f"Total time taken: {end - start:.2f} seconds")
    print(f"Total events processed: {sum(fast_counts) + sum(slow_counts)}")


if __name__ == "__main__":
    main()
