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
finish_event = False

# Tracking sends and receives
fast_recv_counts = 0
fast_sent_counts = 0
slow_recv_counts = 0
slow_sent_counts = 0
parser_sent_fast = 0
parser_sent_slow = 0
fast_steals = 0
slow_steals = 0
reducer_recv_count = 0


bp_time = 0.0
no_bp_time = 0.0

fast_bp_time = 0.0
fast_no_bp_time = 0.0

slow_bp_time = 0.0
slow_no_bp_time = 0.0

start_time = time.time()
last_change = last_change_fast = last_change_slow = start_time
last_state = fast_last_state = slow_last_state = False


def bp_monitor(sample_interval=0.05):
    global \
        bp_time, \
        no_bp_time, \
        last_change, \
        last_state, \
        fast_bp_time, \
        fast_no_bp_time, \
        last_change_fast, \
        fast_last_state, \
        slow_bp_time, \
        slow_no_bp_time, \
        slow_last_state, \
        last_change_slow

    while True:
        time.sleep(sample_interval)
        now = time.time()

        # 1) overall
        overall_in_bp = (fast_queue.qsize() + slow_queue.qsize()) > 0
        if overall_in_bp != last_state:
            delta = now - last_change
            if last_state:
                bp_time += delta
            else:
                no_bp_time += delta
            last_change = now
            last_state = overall_in_bp

        # 2) fast queue only
        fast_in_bp = fast_queue.qsize() > 0
        if fast_in_bp != fast_last_state:
            delta = now - last_change_fast
            if fast_last_state:
                fast_bp_time += delta
            else:
                fast_no_bp_time += delta
            last_change_fast = now
            fast_last_state = fast_in_bp

        # 3) slow queue only
        slow_in_bp = slow_queue.qsize() > 0
        if slow_in_bp != slow_last_state:
            delta = now - last_change_slow
            if slow_last_state:
                slow_bp_time += delta
            else:
                slow_no_bp_time += delta
            last_change_slow = now
            slow_last_state = slow_in_bp


def print_stats():
    global \
        slow_steals, \
        fast_steals, \
        fast_recv_counts, \
        fast_sent_counts, \
        slow_recv_counts, \
        slow_sent_counts

    # overall %
    total = bp_time + no_bp_time
    pct_bp = (bp_time / total * 100) if total > 0 else 0.0

    # fast queue %
    fast_total = fast_bp_time + fast_no_bp_time
    fast_pct_bp = (fast_bp_time / fast_total * 100) if fast_total > 0 else 0.0

    # slow queue %
    slow_total = slow_bp_time + slow_no_bp_time
    slow_pct_bp = (slow_bp_time / slow_total * 100) if slow_total > 0 else 0.0

    elapsed = time.time() - start_time
    overall_throughput = reducer_recv_count / elapsed
    fast_throughput = fast_sent_counts / elapsed
    slow_throughput = slow_sent_counts / elapsed

    print("\n--- Periodic Stats ---")
    print(f"  Parser - Sent: {parser_sent_fast + parser_sent_slow}")
    print(f"  Fast Workers - Recv: {fast_recv_counts} | Sent: {fast_sent_counts}")
    print(f"  Slow Workers - Recv: {slow_recv_counts} | Sent: {slow_sent_counts}")
    print(f"  Reducer - Received: {reducer_recv_count}")
    print("\n------------------------\n")
    print(f"  Overall back-pressure      : {bp_time:.2f}s ({pct_bp:.1f}%)")
    print(f"  Fast back-pressure   : {fast_bp_time:.2f}s ({fast_pct_bp:.1f}%)")
    print(f"  Slow back-pressure   : {slow_bp_time:.2f}s ({slow_pct_bp:.1f}%)")
    print("\n------------------------\n")
    print(f"  Fast steals: {fast_steals}")
    print(f"  Slow steals: {slow_steals}")
    print("\n------------------------\n")
    print(f"  Overall throughput: {overall_throughput}")
    print(f"  Fast throughput: {fast_throughput}")
    print(f"  Slow throughput: {slow_throughput}")
    print("\n------------------------\n")

    fast_steals = 0
    slow_steals = 0


def stat_printer():
    while not finish_event:
        time.sleep(30)
        print_stats()


def parser_generator(events: list[str]):
    global parser_sent_fast, parser_sent_slow, finish_event
    for raw in events:
        event_id, durations = raw.strip().split(",")
        for duration in map(int, durations.split()):
            if duration <= THRESHOLD:
                fast_queue.put_nowait((event_id, duration))
                parser_sent_fast += 1
            else:
                slow_queue.put_nowait((event_id, duration))
                parser_sent_slow += 1

    finish_event = True


def fast_worker(i: int):
    global slow_steals, fast_recv_counts, fast_sent_counts

    while True:
        try:
            item = fast_queue.get_nowait()
            fast_recv_counts += 1
        except queue.Empty:
            try:
                item = slow_queue.get_nowait()
                fast_recv_counts += 1
                slow_steals += 1
            except queue.Empty:
                if finish_event:
                    break
                continue

        time.sleep(item[1] / 1000)
        reducer_queue.put_nowait(item)
        fast_sent_counts += 1


def slow_worker(i: int):
    global fast_steals, slow_recv_counts, slow_sent_counts

    while True:
        try:
            item = slow_queue.get_nowait()
            slow_recv_counts += 1
        except queue.Empty:
            try:
                item = fast_queue.get_nowait()
                slow_recv_counts += 1
                fast_steals += 1
            except queue.Empty:
                if finish_event:
                    break
                continue

        time.sleep(item[1] / 1000)
        reducer_queue.put_nowait(item)
        slow_sent_counts += 1


def reducer_worker():
    global reducer_recv_count
    while not finish_event:
        try:
            reducer_queue.get(timeout=5)
        except queue.Empty:
            continue
        reducer_recv_count += 1


def main():
    start = time.time()

    with open("../dspbench-flink/data/highvariance.csv") as f:
        events = f.readlines()

    bp_monitor_thread = threading.Thread(target=bp_monitor)
    bp_monitor_thread.start()

    printer_thread = threading.Thread(target=stat_printer)
    printer_thread.start()

    fast_threads_list = []
    for i in range(FAST_THREADS):
        t = threading.Thread(target=fast_worker, args=(i,))
        t.start()
        fast_threads_list.append(t)

    slow_threads_list = []
    for i in range(SLOW_THREADS):
        t = threading.Thread(target=slow_worker, args=(i,))
        t.start()
        slow_threads_list.append(t)

    reducer = threading.Thread(target=reducer_worker)
    reducer.start()

    parser_generator(events)

    for t in fast_threads_list + slow_threads_list:
        t.join()

    reducer.join()

    print_stats()

    end = time.time()
    print(f"Total time taken: {end - start:.2f} seconds")
    print(f"Total events processed: {fast_sent_counts + slow_sent_counts}")


if __name__ == "__main__":
    main()
