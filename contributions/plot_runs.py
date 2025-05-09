import glob
import os
import re

import matplotlib.pyplot as plt
import pandas as pd

# Regex to capture config from directory name
CONFIG_RE = re.compile(r"run-(\d+)fast-(\d+)slow-(\d+)threshold")
# Regexes for parsing metrics
PATTERN = re.compile(
    r"""
\s*------------------------\s*
\s*\s*Parser\s*\s*\s*\s*\s*\s*\s*-\s*Sent:\s*(\d+)\s*
\s*\s*Fast\s*Workers\s*-\s*Recv:\s*(\d+)\s*|\s*Sent:\s*(\d+)\s*
\s*\s*Slow\s*Workers\s*-\s*Recv:\s*(\d+)\s*|\s*Sent:\s*(\d+)\s*
\s*\s*Reducer\s*\s*\s*\s*\s*\s*-\s*Recv:\s*(\d+)\s*
\s*------------------------\s*
\s*\s*Overall\s*back-pressure\s*:\s*(\d+\.\d+)s\s*\((\d+\.\d+)\%\)\s*
\s*\s*Fast\s*back-pressure\s*\s*\s*\s*:\s*(\d+\.\d+)s\s*\((\d+\.\d+)\%\)\s*
\s*\s*Slow\s*back-pressure\s*\s*\s*\s*:\s*(\d+\.\d+)s\s*\((\d+\.\d+)\%\)\s*
\s*------------------------\s*
\s*\s*Fast\s*steals:\s*(\d+)\s*
\s*\s*Slow\s*steals:\s*(\d+)\s*
\s*------------------------\s*
\s*\s*Overall\s*throughput\s*:\s*(\d+\.\d+)\s*
\s*\s*Fast\s*throughput\s*\s*\s*\s*:\s*(\d+\.\d+)\s*
\s*\s*Slow\s*throughput\s*\s*\s*\s*:\s*(\d+\.\d+)\s*
------------------------\s*
""".strip()
)


def parse_config(dirname):
    m = CONFIG_RE.match(os.path.basename(dirname))
    if not m:
        return None
    return {
        "fast_threads": int(m.groups()[0]),
        "slow_threads": int(m.groups()[1]),
        "threshold": int(m.groups()[2]),
    }


def parse_snapshot(file_path, file_idx: int):
    labels = [
        "parser_sent",
        "fast_recv",
        "fast_sent",
        "slow_recv",
        "slow_sent",
        "reducer_recv",
        "overall_bp_time",
        "overall_bp_pct",
        "fast_bp_time",
        "fast_bp_pct",
        "slow_bp_time",
        "slow_bp_pct",
        "fast_steals",
        "slow_steals",
        "overall_throughput",
        "fast_throughput",
        "slow_throughput",
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
    pattern = os.path.join(base_dir, "run-*fast-*slow-*")
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
    # Unique thresholds and combos
    thresholds = sorted(df["threshold"].unique())
    combos = (
        df[["fast_threads", "slow_threads"]]
        .drop_duplicates()
        .sort_values(["fast_threads", "slow_threads"])
        .reset_index(drop=True)
    )
    combos["total_threads"] = combos["fast_threads"] + combos["slow_threads"]

    # Color per total threads
    totals = sorted(combos["total_threads"].unique())
    palette = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    color_map = {tot: palette[i % len(palette)] for i, tot in enumerate(totals)}

    # Marker per group (fast > slow, slow > fast, equal)
    group_markers = {"more_fast": "o", "more_slow": "x", "equal": "^"}

    def get_marker(ft, st):
        if ft > st:
            return group_markers["more_fast"]
        elif st > ft:
            return group_markers["more_slow"]
        else:
            return group_markers["equal"]

    # Legend sorter by total
    def regroup_legend(ax):
        handles, labels = ax.get_legend_handles_labels()
        items = []
        for h, lbl in zip(handles, labels):
            try:
                tot = int(lbl.split("(")[-1].split()[0])
            except Exception:
                tot = 0
            items.append((tot, h, lbl))
        items.sort(key=lambda x: x[0])
        sorted_handles = [h for _, h, _ in items]
        sorted_labels = [lbl for _, _, lbl in items]
        ax.legend(
            sorted_handles,
            sorted_labels,
            title="Thread Combos",
            bbox_to_anchor=(1.05, 1),
            loc="upper left",
        )

    # 1. Throughput over time per threshold
    for thr in thresholds:
        plt.close("all")
        fig, ax = plt.subplots()
        for _, row in combos.iterrows():
            ft, st, tot = row.fast_threads, row.slow_threads, row.total_threads
            sel = df[
                (df["threshold"] == thr)
                & (df["fast_threads"] == ft)
                & (df["slow_threads"] == st)
            ]
            if sel.empty:
                continue
            sel = sel.sort_values("time")
            ax.plot(
                sel["time"],
                sel["overall_throughput"],
                color=color_map[tot],
                marker=get_marker(ft, st),
                label=f"{ft}F/{st}S ({tot} total)",
            )
        ax.set_title(f"Overall Throughput (Threshold={thr})")
        ax.set_xlabel("Time")
        ax.set_ylabel("Records/sec")
        regroup_legend(ax)
        plt.tight_layout()
        plt.show()

    # 2. Fast vs Slow throughput at median threshold
    plt.close("all")
    mid = thresholds[len(thresholds) // 2]
    sub = df[df["threshold"] == mid].sort_values("time")
    fig, ax = plt.subplots()
    ax.plot(
        sub["time"], sub["fast_throughput"], marker="o", linestyle="-", label="Fast"
    )
    ax.plot(
        sub["time"], sub["slow_throughput"], marker="s", linestyle="--", label="Slow"
    )
    ax.set_title(f"Fast vs Slow Throughput (Threshold={mid})")
    ax.set_xlabel("Time")
    ax.set_ylabel("Records/sec")
    ax.legend()
    plt.tight_layout()
    plt.show()

    # 3. Backpressure % over time per threshold
    for thr in thresholds:
        plt.close("all")
        fig, ax = plt.subplots()
        sub = df[df["threshold"] == thr].sort_values("time")
        ax.plot(
            sub["time"],
            sub["fast_bp_pct"],
            marker="x",
            linestyle="--",
            label="Fast BP %",
        )
        ax.plot(
            sub["time"],
            sub["slow_bp_pct"],
            marker="x",
            linestyle="-.",
            label="Slow BP %",
        )
        ax.set_title(f"Backpressure % (Threshold={thr})")
        ax.set_xlabel("Time")
        ax.set_ylabel("%")
        ax.legend()
        plt.tight_layout()
        plt.show()

    # 4. Final throughput vs threshold
    plt.close("all")
    final = (
        df.sort_values("time")
        .groupby(["threshold", "fast_threads", "slow_threads"])
        .last()
        .reset_index()
    )
    fig, ax = plt.subplots()
    for _, row in combos.iterrows():
        ft, st, tot = row.fast_threads, row.slow_threads, row.total_threads
        sel = final[(final["fast_threads"] == ft) & (final["slow_threads"] == st)]
        if sel.empty:
            continue
        ax.plot(
            sel["threshold"],
            sel["overall_throughput"],
            color=color_map[tot],
            marker=get_marker(ft, st),
            linestyle="-",
        )
    ax.set_title("Final Throughput vs Threshold")
    ax.set_xlabel("Threshold")
    ax.set_ylabel("Records/sec")
    regroup_legend(ax)
    plt.tight_layout()
    plt.show()

    # 5. Total steals over time per threshold
    for thr in thresholds:
        plt.close("all")
        fig, ax = plt.subplots()
        for _, row in combos.iterrows():
            ft, st, tot = row.fast_threads, row.slow_threads, row.total_threads
            sel = df[
                (df["threshold"] == thr)
                & (df["fast_threads"] == ft)
                & (df["slow_threads"] == st)
            ]
            if sel.empty:
                continue
            sel = sel.sort_values("time")
            total_steals = sel["fast_steals"] + sel["slow_steals"]
            ax.plot(
                sel["time"],
                total_steals,
                color=color_map[tot],
                marker=get_marker(ft, st),
                label=f"{ft}F/{st}S ({tot} total)",
            )
        ax.set_title(f"Total Steals over Time (Threshold={thr})")
        ax.set_xlabel("Time")
        ax.set_ylabel("Number of Steals")
        regroup_legend(ax)
        plt.tight_layout()
        plt.show()


def main():
    df = collect_data(".")
    if df.empty:
        print("No data found.")
        return

    # print(list(df.columns))
    # for column in df.columns:
    #     print(df[column].describe())

    plot(df)


if __name__ == "__main__":
    main()
