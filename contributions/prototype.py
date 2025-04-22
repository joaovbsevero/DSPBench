import queue
import threading
import time

# Settings
NUM_COLLECTORS = 16  # parallelism for the "collect" stage

# Queues for inter-stage communication
collector_queues = [queue.Queue() for _ in range(NUM_COLLECTORS)]
reducer_queue = queue.Queue()

# Stats for demonstration
collector_counts = [0] * NUM_COLLECTORS


# --- Pipeline components ---


def parser_generator(events: list[str]):
    """FlatMap(parse) -> FlatMap(generate) -> rebalance -> collectors."""
    for idx, raw in enumerate(events):
        event_id, durations = raw.split(",")

        # Round-robin assignment
        for duration in map(int, durations.split()):
            target = idx % NUM_COLLECTORS
            collector_queues[target].put((event_id, duration))

    # signal end-of-stream to collectors
    for q in collector_queues:
        q.put(None)


def collector_worker(i: int):
    """Map(collect) -> forward to reducer."""
    while True:
        item = collector_queues[i].get()
        if item is None:
            break

        time.sleep(item[1] / 1000)
        collector_counts[i] += 1
        reducer_queue.put(item)

    # signal end-of-stream to reducer
    reducer_queue.put(None)


def reducer_worker():
    """Keyed reduce -> simulate single-threaded reducer."""
    end_signals = 0
    results = {}
    while True:
        item = reducer_queue.get()
        if item is None:
            end_signals += 1
            if end_signals == NUM_COLLECTORS:
                break
        else:
            results[item[0]] = results.get(item[0], 0) + 1


# --- Main simulation ---


def main():
    start = time.time()

    # Generate synthetic events with random keys
    with open("../dspbench-flink/data/highvariance.csv") as f:
        events = f.readlines()

    # Start collector threads
    collectors: list[threading.Thread] = []
    for i in range(NUM_COLLECTORS):
        t = threading.Thread(target=collector_worker, args=(i,))
        t.start()
        collectors.append(t)

    # Start reducer thread
    reducer = threading.Thread(target=reducer_worker)
    reducer.start()

    # Run parser + generator in this (single) thread
    parser_generator(events)

    # Wait for all collectors to finish
    for t in collectors:
        t.join()

    # Wait for reducer to finish
    reducer.join()

    # Show how many events each collector handled
    print("Collector workload distribution:")
    for i, cnt in enumerate(collector_counts):
        print(f"  collector_{i}: {cnt} events")

    end = time.time()
    print(f"Total time taken: {end - start:.2f} seconds")
    print(f"Total events processed: {sum(collector_counts)}")


if __name__ == "__main__":
    main()
