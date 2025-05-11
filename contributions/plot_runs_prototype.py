import glob
import os
import re

import matplotlib.pyplot as plt
import pandas as pd

# Regex to capture config from directory name
CONFIG_RE = re.compile(r"run-(\d+)$")
# Regexes for parsing metrics
PATTERN = re.compile(
    r"""
\s*------------------------\s*
\s*Parser\s*-\s*Sent:\s*(\d+)\s*
\s*Collectors\s*-\s*Recv:\s*(\d+)\s*|\s*Sent:\s*(\d+)\s*
\s*Reducer\s*-\s*Recv:\s*(\d+)\s*
\s*------------------------\s*
\s*Back-pressure\s*:\s*(\d+\.\d+)s\s*\((\d+\.\d+)\%\)\s*
\s*Throughput\s*:\s*(\d+\.\d+)\s*
------------------------\s*
""".strip()
)


def parse_config(dirname):
    m = CONFIG_RE.match(os.path.basename(dirname))
    if not m:
        return None
    return {
        "threads": int(m.groups()[0]),
    }


def parse_snapshot(file_path, file_idx: int):
    labels = [
        "parser_sent",
        "recv",
        "sent",
        "reducer_recv",
        "bp_time",
        "bp_pct",
        "throughput",
    ]
    metrics = {"time": file_idx}
    with open(file_path, "r") as f:
        content = f.read()
        m = PATTERN.findall(content)
        values = [0.0] * 17
        for v in m:
            for idx, g in enumerate(v):
                if g:
                    values[idx] = float(g)
        for label, value in zip(labels, values):
            metrics[label] = value  # type: ignore
    return metrics


def collect_data(base_dir):
    rows = []
    pattern = os.path.join(base_dir, "run-*")
    for dirname in sorted(glob.glob(pattern)):
        cfg = parse_config(dirname)
        if not cfg:
            continue
        snaps = sorted(
            glob.glob(os.path.join(dirname, "*")),
            key=lambda p: int(re.search(r"(\d+)", os.path.basename(p)).group(1)),  # type: ignore
        )
        for idx, snap in enumerate(snaps):
            data = parse_snapshot(snap, idx)
            data.update(cfg)
            rows.append(data)
    return pd.DataFrame(rows)


def plot(df: pd.DataFrame):
    threads = sorted(df["threads"].unique())
    palette = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    color_map = {t: palette[i % len(palette)] for i, t in enumerate(threads)}

    plt.close("all")

    # 1. Throughput over time
    fig, ax = plt.subplots()
    for t in threads:
        sel = df[df["threads"] == t].sort_values("time")
        ax.plot(
            sel["time"],
            sel["throughput"],
            color=color_map[t],
            marker="o",
            label=f"{t} threads",
        )
    ax.set_title("Original Throughput over Time")
    ax.set_xlabel("Time")
    ax.set_ylabel("Records/sec")
    ax.legend(title="Threads", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    plt.savefig("base_plots/Original Throughput over Time.png")

    plt.close("all")

    # 2. Backpressure % over time
    fig, ax = plt.subplots()
    for t in threads:
        sel = df[df["threads"] == t].sort_values("time")
        ax.plot(
            sel["time"],
            sel["bp_pct"],
            color=color_map[t],
            marker="o",
            label=f"{t} threads",
        )
    ax.set_title("Original Backpressure % over Time")
    ax.set_xlabel("Time")
    ax.set_ylabel("%")
    ax.legend(title="Threads", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    plt.savefig("base_plots/Original Backpressure % over Time.png")

    plt.close("all")

    # 3. Final throughput vs threads
    final = df.sort_values("time").groupby("threads").last().reset_index()
    fig, ax = plt.subplots()
    ax.plot(final["threads"], final["throughput"], marker="o")
    ax.set_title("Original Final Throughput vs Threads")
    ax.set_xlabel("Threads")
    ax.set_ylabel("Records/sec")
    plt.tight_layout()
    plt.savefig("base_plots/Original Final Throughput vs Threads.png")

    plt.close("all")


def main():
    import pathlib

    df = collect_data(str(pathlib.Path(__file__).parent))
    if df.empty:
        print("No data found.")
        return

    # print(list(df.columns))
    # for column in df.columns:
    #     print(df[column].describe())

    plot(df)


if __name__ == "__main__":
    main()
