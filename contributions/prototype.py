import pathlib
import queue
import threading
import time

import typer

app = typer.Typer()

# Settings
NUM_COLLECTORS = 0  # parallelism for the "collect" stage
FOLDER = pathlib.Path()

# Queues for inter-stage communication
collector_queue = queue.Queue()
reducer_queue = queue.Queue()

# Event to signal no more incoming tasks
finish_event = False

# Stats for demonstration
collector_counts = 0
collector_recv_counts = 0
collector_sent_counts = 0
parser_sent_counts = 0
reducer_recv_count = 0
bp_time = 0.0
no_bp_time = 0.0
start_time = time.time()
last_change = start_time
last_state = False


def stat_printer():
    while True:
        time.sleep(30)

        timestamp = time.time()

        # overall %
        total = bp_time + no_bp_time
        pct_bp = (bp_time / total * 100) if total > 0 else 0.0

        elapsed = timestamp - start_time
        overall_throughput = reducer_recv_count / elapsed

        log = (
            "------------------------\n"
            f"  Parser     - Sent: {parser_sent_counts}\n"
            f"  Collectors - Recv: {collector_recv_counts} | Sent: {collector_sent_counts}\n"
            f"  Reducer    - Recv: {reducer_recv_count}\n"
            "------------------------\n"
            f"  Back-pressure : {bp_time:.2f}s ({pct_bp:.1f}%)\n"
            f"  Throughput    : {overall_throughput}\n"
            "------------------------\n"
        )
        with open(FOLDER / f"{timestamp}.txt", "w") as f:
            f.write(log)


def bp_monitor(sample_interval=0.05):
    global bp_time, no_bp_time, last_change, last_state

    while True:
        time.sleep(sample_interval)

        # print("Running bp monitor beat")
        now = time.time()

        overall_in_bp = collector_queue.qsize() > 0
        if overall_in_bp != last_state:
            delta = now - last_change
            if last_state:
                bp_time += delta
            else:
                no_bp_time += delta

            last_change = now
            last_state = overall_in_bp


def parser_generator(events: list[str]):
    global parser_sent_counts

    for raw in events:
        event_id, durations = raw.strip().split(",")
        for duration in map(int, durations.split()):
            collector_queue.put_nowait((event_id, duration))
            parser_sent_counts += 1

    for _ in range(NUM_COLLECTORS):
        collector_queue.put_nowait(None)

    # print("Finished generating events")


def collector_worker():
    global \
        collector_recv_counts, \
        collector_sent_counts, \
        finish_event, \
        collector_queue, \
        reducer_queue

    while True:
        try:
            item = collector_queue.get_nowait()
            if item is None:
                reducer_queue.put_nowait(item)
                continue

            # print("Collector received event")

        except queue.Empty:
            if finish_event:
                break
            continue

        collector_recv_counts += 1
        time.sleep(item[1] / 1000)
        reducer_queue.put_nowait(item)
        # print("Collector added event to reducer")
        collector_sent_counts += 1

    # print("Collector finished")


def reducer_worker():
    global reducer_recv_count, finish_event

    none_count = 0
    while none_count < NUM_COLLECTORS:
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
def run(threads: int = typer.Option(..., "-t")):
    global NUM_COLLECTORS, FOLDER

    NUM_COLLECTORS = threads

    FOLDER = pathlib.Path(__file__).parent / f"run-{threads}"
    if not FOLDER.exists():
        FOLDER.mkdir(parents=True, exist_ok=True)
    else:
        # Clean dir
        [item.unlink() for item in FOLDER.iterdir() if item.is_file()]

    with open("../dspbench-flink/data/highvariance.csv") as f:
        events = f.readlines()

    threading.Thread(target=bp_monitor, daemon=True).start()
    threading.Thread(target=stat_printer, daemon=True).start()

    for i in range(NUM_COLLECTORS):
        threading.Thread(target=collector_worker).start()

    threading.Thread(target=reducer_worker).start()

    parser_generator(events)


if __name__ == "__main__":
    app()
