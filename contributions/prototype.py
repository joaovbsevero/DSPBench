import queue
import threading
import time

# Settings
NUM_COLLECTORS = 4  # parallelism for the "collect" stage

# Queues for inter-stage communication
collector_queues = [queue.Queue() for _ in range(NUM_COLLECTORS)]
reducer_queue = queue.Queue()

# Stats for demonstration
collector_counts = [0] * NUM_COLLECTORS
collector_recv_counts = [0] * NUM_COLLECTORS
collector_sent_counts = [0] * NUM_COLLECTORS
parser_sent_counts = [0] * NUM_COLLECTORS
reducer_recv_count = 0

# Lock for printing
print_lock = threading.Lock()


def print_stats():
    with print_lock:
        print("\n--- Periodic Stats ---")
        print(f"Parser - Sent: {sum(parser_sent_counts)}")
        print(
            f"Collectors - Recv: {sum(collector_recv_counts)} | Sent: {sum(collector_sent_counts)}"
        )
        print(f"Reducer - Recv: {reducer_recv_count}")


def periodic_printer():
    while True:
        time.sleep(60)
        print_stats()


def parser_generator(events: list[str]):
    for idx, raw in enumerate(events):
        event_id, durations = raw.strip().split(",")

        for duration in map(int, durations.split()):
            target = idx % NUM_COLLECTORS
            collector_queues[target].put((event_id, duration))
            parser_sent_counts[target] += 1

    for q in collector_queues:
        q.put(None)


def collector_worker(i: int):
    while True:
        item = collector_queues[i].get()
        if item is None:
            break

        collector_recv_counts[i] += 1
        time.sleep(item[1] / 1000)
        collector_counts[i] += 1
        reducer_queue.put(item)
        collector_sent_counts[i] += 1

    reducer_queue.put(None)


def reducer_worker():
    global reducer_recv_count
    end_signals = 0
    results = {}
    while True:
        item = reducer_queue.get()
        if item is None:
            end_signals += 1
            if end_signals == NUM_COLLECTORS:
                break
        else:
            reducer_recv_count += 1
            event_id = item[0]
            results[event_id] = results.get(event_id, 0) + 1


def main():
    start = time.time()

    with open("../dspbench-flink/data/highvariance.csv") as f:
        events = f.readlines()

    printer_thread = threading.Thread(target=periodic_printer, daemon=True)
    printer_thread.start()

    collectors: list[threading.Thread] = []
    for i in range(NUM_COLLECTORS):
        t = threading.Thread(target=collector_worker, args=(i,))
        t.start()
        collectors.append(t)

    reducer = threading.Thread(target=reducer_worker)
    reducer.start()

    parser_generator(events)

    for t in collectors:
        t.join()

    reducer.join()

    print_stats()

    end = time.time()
    print(f"Total time taken: {end - start:.2f} seconds")
    print(f"Total events processed: {sum(collector_counts)}")


if __name__ == "__main__":
    main()
