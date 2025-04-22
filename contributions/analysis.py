#!/usr/bin/env python3
"""
Script: plot_dataset_statistics.py

Loads a synthetic Flink dataset CSV and plots various statistics:
 1. Histogram of all durations
 2. Boxplot of durations
 3. Histogram of search counts per event
 4. Scatter: search-count vs average duration
 5. Empirical CDF of durations

Each row in the CSV is expected as:
ID, Name, Content, duration1, duration2, ..., durationN
"""

import os

import matplotlib.pyplot as plt
import numpy as np


def load_dataset(path):
    durations = []
    search_counts = []
    avg_durations = []
    with open(path, "r", newline="") as f:
        for row in f.readlines():
            _, row_durations = row.split(",")
            # parse durations as integers
            ds = list(map(int, row_durations.split()))
            durations.extend(ds)
            count = len(ds)
            search_counts.append(count)
            avg = np.mean(ds) if count > 0 else np.nan
            avg_durations.append(avg)
    return np.array(durations), np.array(search_counts), np.array(avg_durations)


def save_plot(fig_name):
    plt.tight_layout()
    plt.savefig(fig_name)
    plt.close()


def plot_histogram(durations, out_dir="."):
    plt.figure()
    plt.hist(durations, bins=100, density=True)
    plt.title("Duration Histogram")
    plt.xlabel("Duration (ms)")
    plt.ylabel("Probability Density")
    save_plot(os.path.join(out_dir, "duration_histogram.png"))


def plot_boxplot(durations, out_dir="."):
    plt.figure()
    plt.boxplot(durations, vert=False)
    plt.title("Duration Boxplot")
    plt.xlabel("Duration (ms)")
    save_plot(os.path.join(out_dir, "duration_boxplot.png"))


def plot_search_count_hist(search_counts, out_dir="."):
    plt.figure()
    bins = range(0, int(search_counts.max()) + 2)
    plt.hist(search_counts, bins=bins, align="left", rwidth=0.8)
    plt.title("Search Count per Event")
    plt.xlabel("Number of Searches")
    plt.ylabel("Frequency")
    save_plot(os.path.join(out_dir, "search_count_histogram.png"))


def plot_scatter(search_counts, avg_durations, out_dir="."):
    plt.figure()
    valid = ~np.isnan(avg_durations)
    plt.scatter(search_counts[valid], avg_durations[valid], alpha=0.5)
    plt.title("Search Count vs Avg Duration")
    plt.xlabel("Search Count")
    plt.ylabel("Average Duration (ms)")
    save_plot(os.path.join(out_dir, "count_vs_avg_scatter.png"))


def plot_cdf(durations, out_dir="."):
    plt.figure()
    sorted_d = np.sort(durations)
    y = np.arange(1, len(sorted_d) + 1) / len(sorted_d)
    plt.plot(sorted_d, y)
    plt.title("Empirical CDF of Durations")
    plt.xlabel("Duration (ms)")
    plt.ylabel("CDF")
    save_plot(os.path.join(out_dir, "duration_cdf.png"))


def main(path: str):
    durations, search_counts, avg_durations = load_dataset(path)

    plot_histogram(durations)
    plot_boxplot(durations)
    plot_search_count_hist(search_counts)
    plot_scatter(search_counts, avg_durations)
    plot_cdf(durations)

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main("../dspbench-flink/data/highvariance.csv")
