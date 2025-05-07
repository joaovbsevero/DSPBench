import pathlib
import queue
import threading
import time

import typer

app = typer.Typer()


# Settings
FAST_THREADS = 0  # number of fast-task workers
SLOW_THREADS = 0  # number of slow-task workers
THRESHOLD = -1
FOLDER = pathlib.Path()

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
        # print("Running bp monitor beat")

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


def stat_printer():
    global \
        slow_steals, \
        fast_steals, \
        fast_recv_counts, \
        fast_sent_counts, \
        slow_recv_counts, \
        slow_sent_counts

    while not finish_event:
        time.sleep(30)

        timestamp = time.time()

        # overall %
        total = bp_time + no_bp_time
        pct_bp = (bp_time / total * 100) if total > 0 else 0.0

        # fast queue %
        fast_total = fast_bp_time + fast_no_bp_time
        fast_pct_bp = (fast_bp_time / fast_total * 100) if fast_total > 0 else 0.0

        # slow queue %
        slow_total = slow_bp_time + slow_no_bp_time
        slow_pct_bp = (slow_bp_time / slow_total * 100) if slow_total > 0 else 0.0

        elapsed = timestamp - start_time
        overall_throughput = reducer_recv_count / elapsed
        fast_throughput = fast_sent_counts / elapsed
        slow_throughput = slow_sent_counts / elapsed

        log = (
            "------------------------\n"
            f"  Parser       - Sent: {parser_sent_fast + parser_sent_slow}\n"
            f"  Fast Workers - Recv: {fast_recv_counts} | Sent: {fast_sent_counts}\n"
            f"  Slow Workers - Recv: {slow_recv_counts} | Sent: {slow_sent_counts}\n"
            f"  Reducer      - Recv: {reducer_recv_count}\n"
            "------------------------\n"
            f"  Overall back-pressure : {bp_time:.2f}s ({pct_bp:.1f}%)\n"
            f"  Fast back-pressure    : {fast_bp_time:.2f}s ({fast_pct_bp:.1f}%)\n"
            f"  Slow back-pressure    : {slow_bp_time:.2f}s ({slow_pct_bp:.1f}%)\n"
            "------------------------\n"
            f"  Fast steals: {fast_steals}\n"
            f"  Slow steals: {slow_steals}\n"
            "------------------------\n"
            f"  Overall throughput : {overall_throughput}\n"
            f"  Fast throughput    : {fast_throughput}\n"
            f"  Slow throughput    : {slow_throughput}"
            "------------------------\n"
        )
        with open(FOLDER / f"{timestamp}.txt", "w") as f:
            f.write(log)

        fast_steals = 0
        slow_steals = 0


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

    for _ in range(FAST_THREADS):
        fast_queue.put_nowait(None)

    for _ in range(SLOW_THREADS):
        slow_queue.put_nowait(None)

    # print("Finished generating events")


def fast_worker():
    global slow_steals, fast_recv_counts, fast_sent_counts

    while True:
        try:
            item = fast_queue.get_nowait()
            if item is None:
                reducer_queue.put_nowait(item)
                continue

            fast_recv_counts += 1
            # print("Fast received event")
        except queue.Empty:
            try:
                item = slow_queue.get_nowait()
                if item is None:
                    reducer_queue.put_nowait(item)
                    continue

                fast_recv_counts += 1
                slow_steals += 1
                # print("Fast stole event")
            except queue.Empty:
                if finish_event:
                    break
                continue

        time.sleep(item[1] / 1000)
        reducer_queue.put_nowait(item)
        # print("Fast added event to reducer")
        fast_sent_counts += 1

    # print("Fast finished")


def slow_worker():
    global fast_steals, slow_recv_counts, slow_sent_counts

    while True:
        try:
            item = slow_queue.get_nowait()
            if item is None:
                reducer_queue.put_nowait(item)
                continue

            # print("Slow received event")
        except queue.Empty:
            try:
                item = fast_queue.get_nowait()
                if item is None:
                    reducer_queue.put_nowait(item)
                    continue

                fast_steals += 1
                # print("Slow stole event")
            except queue.Empty:
                if finish_event:
                    break
                continue

        slow_recv_counts += 1
        time.sleep(item[1] / 1000)
        reducer_queue.put_nowait(item)
        # print("Slow added event to reducer")
        slow_sent_counts += 1

    # print("Slow finished")


def reducer_worker():
    global reducer_recv_count, finish_event
    none_count = 0
    while none_count < (FAST_THREADS + SLOW_THREADS):
        try:
            item = reducer_queue.get(timeout=5)
            if item is None:
                none_count += 1
                continue
            # print("Reducer received event")
        except queue.Empty:
            continue
        reducer_recv_count += 1

    # print("Reducer finished")
    finish_event = True


@app.command()
def run(
    fast_thread_count: int = typer.Option(..., "--fast-count"),
    slow_thread_count: int = typer.Option(..., "--slow-count"),
    threashold: float = typer.Option(..., "-t"),
):
    global FAST_THREADS, SLOW_THREADS, THRESHOLD, FOLDER

    FAST_THREADS = fast_thread_count
    SLOW_THREADS = slow_thread_count
    THRESHOLD = threashold

    FOLDER = (
        pathlib.Path(__file__).parent
        / f"run-{fast_thread_count}fast-{slow_thread_count}slow-{threashold}threashold"
    )
    if not FOLDER.exists():
        FOLDER.mkdir(parents=True, exist_ok=True)
    else:
        # Clean dir
        [item.unlink() for item in FOLDER.iterdir() if item.is_file()]

    with open("../dspbench-flink/data/highvariance.csv") as f:
        events = f.readlines()

    threading.Thread(target=bp_monitor, daemon=True).start()
    threading.Thread(target=stat_printer, daemon=True).start()

    for _ in range(FAST_THREADS):
        threading.Thread(target=fast_worker, daemon=False).start()

    for _ in range(SLOW_THREADS):
        threading.Thread(target=slow_worker, daemon=False).start()

    threading.Thread(target=reducer_worker, daemon=False).start()

    parser_generator(events)


if __name__ == "__main__":
    app()
