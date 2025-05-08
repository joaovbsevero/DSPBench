#!/usr/bin/env python3
"""
Script to parse performance snapshots and plot metrics for different thread pools and threashold configurations.

Usage:
    python plot_metrics.py --dir /path/to/your/data

Assumptions:
- Directories are named like: run-<fast>fast-<slow>slow-<threashold>threashold
- Snapshot files are text files in each directory, with a timestamp in the filename (e.g., 60.txt, stats-120.log)
- Each snapshot file contains metric lines as described in the problem statement.
"""

import argparse
import glob
import os
import re

import matplotlib.pyplot as plt
import pandas as pd

# Regex to capture config from directory name
CONFIG_RE = re.compile(r"run-(\d+)fast-(\d+)slow-(\d+).*?threashold")
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
        "threashold": int(m.groups()[2]),
    }


def parse_snapshot(file_path):
    # Extract time from filename: first integer found
    fname = os.path.basename(file_path)
    tmatch = re.search(r"(\d+)", fname)
    timestamp = int(tmatch.group(1)) if tmatch else None

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
    metrics = {"time": timestamp}
    with open(file_path, "r") as f:
        content = f.read()
        m = PATTERN.findall(content)
        values = [0.0] * 17
        for v in m:
            for idx, g in enumerate(v):
                if g:
                    values[idx] = float(g)
        for label, value in zip(labels, values):
            metrics[label] = value
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
            key=lambda p: int(re.search(r"(\d+)", os.path.basename(p)).group(1)),
        )
        for snap in snaps:
            data = parse_snapshot(snap)
            data.update(cfg)
            rows.append(data)
    return pd.DataFrame(rows)


def plot_by_threshold(df, metric_col, title, output_dir):
    for thr in sorted(df["threshold"].unique()):
        sub = df[df["threshold"] == thr]
        if sub.empty:
            continue
        plt.figure()
        for (f, s), grp in sub.groupby(["fast_threads", "slow_threads"]):
            plt.plot(
                grp["time"], grp[metric_col], marker="o", label=f"fast={f}, slow={s}"
            )
        plt.xlabel("Time (s)")
        plt.ylabel(title)
        plt.title(f"{title} (threshold={thr})")
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{metric_col}_threshold_{thr}.png"))
        plt.close()


def plot_by_mode(df, metric_col, title, output_dir):
    dfm = add_mode_column(df)
    for mode in sorted(dfm["mode"].unique()):
        sub = dfm[dfm["mode"] == mode]
        if sub.empty:
            continue
        plt.figure()
        for thr, grp in sub.groupby("threshold"):
            plt.plot(grp["time"], grp[metric_col], marker="o", label=f"thr={thr}")
        plt.xlabel("Time (s)")
        plt.ylabel(title)
        plt.title(f"{title} ({mode})")
        plt.legend()
        safe_mode = mode.replace(">", "gt").replace("<", "lt").replace("=", "eq")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{metric_col}_mode_{safe_mode}.png"))
        plt.close()


def main():
    parser = argparse.ArgumentParser(description="Plot metrics by threshold and mode")
    parser.add_argument("--dir", default=".", help="Base directory containing runs")
    parser.add_argument("--output", default="plots", help="Output directory for plots")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    df = collect_data(args.dir)
    if df.empty:
        print("No data found.")
        return

    metrics = [
        ("overall_throughput", "Overall Throughput"),
        ("fast_throughput", "Fast Throughput"),
        ("slow_throughput", "Slow Throughput"),
        ("overall_bp_pct", "Overall Back-pressure (%)"),
        ("fast_bp_pct", "Fast Back-pressure (%)"),
        ("slow_bp_pct", "Slow Back-pressure (%)"),
        ("fast_steals", "Fast Steals"),
        ("slow_steals", "Slow Steals"),
    ]

    for col, title in metrics:
        plot_by_threshold(df, col, title, args.output)
        plot_by_mode(df, col, title, args.output)


if __name__ == "__main__":
    main()
