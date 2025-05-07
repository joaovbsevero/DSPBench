import math

import matplotlib.pyplot as plt
import numpy as np


def generate_image(payload: dict[str, list[dict[str, int]]], path: str):
    # Grid layout: 2 columns
    configs = list(payload.items())
    num_configs = len(configs)
    cols = 2
    rows = math.ceil(num_configs / cols)

    fig, axes = plt.subplots(nrows=rows, ncols=cols, figsize=(14, rows * 4))
    axes = axes.flatten()
    fig.tight_layout(pad=5)

    for i, (config, stages) in enumerate(configs):
        ax1 = axes[i]
        stage_ids = [f"Stage {i}" for i in range(len(stages))]
        records_sent = [s["Records Sent"] for s in stages]
        parallelism = [s["Parallelism"] for s in stages]
        throughput = [
            s["Records Sent"] / s["Parallelism"] if s["Parallelism"] else 0
            for s in stages
        ]

        # Bar on left axis (log scale)
        bars = ax1.bar(stage_ids, records_sent, alpha=0.7, label="Records Sent")
        ax1.set_yscale("log")
        ax1.set_ylabel("Records Sent (log scale)", color="tab:blue")
        ax1.tick_params(axis="y", labelcolor="tab:blue")

        # Twin axis for the two smaller-series (also log)
        ax2 = ax1.twinx()
        ax2.set_yscale("log")
        (ln1,) = ax2.plot(
            stage_ids,
            parallelism,
            color="tab:orange",
            marker="o",
            label="Parallelism",
            linewidth=2,
        )
        (ln2,) = ax2.plot(
            stage_ids,
            throughput,
            color="tab:green",
            marker="x",
            label="Throughput",
            linewidth=2,
        )
        ax2.set_ylabel("Parallelism / Throughput (log scale)", color="tab:gray")
        ax2.tick_params(axis="y", labelcolor="tab:gray")

        # Combine legends
        lines = [bars, ln1, ln2]
        labels = [l.get_label() for l in lines]
        ax1.legend(lines, labels, fontsize=8, loc="upper left")

        ax1.set_title(config, fontsize=10)
        ax1.set_xlabel("Stages")
        ax1.tick_params(axis="x", rotation=45)

    # Remove unused subplots if any
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.suptitle(
        "Flink Configs (16 / 8 / 6 / 4) – Records Sent, Parallelism, Throughput (log scale)",
        fontsize=16,
    )
    plt.subplots_adjust(top=0.92, hspace=0.4)
    plt.savefig(path, dpi=300, bbox_inches="tight")


def generate_comparison_image(
    proto: dict[str, list[dict[str, int]]],
    impr: dict[str, list[dict[str, int]]],
    path: str,
):
    # assume both dicts have the same keys and same number of stages per config
    configs = list(proto.keys())
    num_configs = len(configs)
    cols = 2
    rows = math.ceil(num_configs / cols)

    fig, axes = plt.subplots(rows, cols, figsize=(14, rows * 4))
    axes = axes.flatten()
    fig.tight_layout(pad=5)

    for idx, config in enumerate(configs):
        ax1 = axes[idx]
        stages_proto = proto[config]
        stages_impr = impr[config]
        n_stages = len(stages_proto)
        stage_labels = [f"Stage {i}" for i in range(n_stages)]
        x = np.arange(n_stages)
        width = 0.35

        # Extract metrics
        rs_p = np.array([s["Records Sent"] for s in stages_proto])
        rs_i = np.array([s["Records Sent"] for s in stages_impr])
        par_p = np.array([s["Parallelism"] for s in stages_proto])
        par_i = np.array([s["Parallelism"] for s in stages_impr])
        th_p = rs_p / par_p
        th_i = rs_i / par_i

        # Bar: Records Sent
        b1 = ax1.bar(x - width / 2, rs_p, width, label="Proto RS", alpha=0.6)
        b2 = ax1.bar(x + width / 2, rs_i, width, label="Impr RS", alpha=0.8)
        ax1.set_yscale("log")
        ax1.set_ylabel("Records Sent (log scale)", color="tab:blue")
        ax1.tick_params(axis="y", labelcolor="tab:blue")

        # Annotate percent improvement on top of improved bars
        for xi, p, i in zip(x, rs_p, rs_i):
            if p > 0:
                pct = (i - p) / p * 100
                ax1.text(
                    xi + width / 2,
                    i * 1.1,
                    f"{pct:.0f}%",
                    ha="center",
                    va="bottom",
                    fontsize=7,
                )

        # Twin axis for Parallelism & Throughput
        ax2 = ax1.twinx()
        ax2.set_yscale("log")
        ln1 = ax2.plot(x, par_p, linestyle="--", marker="o", label="Proto Par")
        ln2 = ax2.plot(x, par_i, linestyle="-", marker="o", label="Impr Par")
        ln3 = ax2.plot(x, th_p, linestyle="--", marker="x", label="Proto Thrpt")
        ln4 = ax2.plot(x, th_i, linestyle="-", marker="x", label="Impr Thrpt")
        ax2.set_ylabel("Parallelism / Throughput (log scale)", color="tab:gray")
        ax2.tick_params(axis="y", labelcolor="tab:gray")

        # Annotate throughput improvement as percentage
        for xi, tp, ti in zip(x, th_p, th_i):
            if tp > 0:
                pct = (ti - tp) / tp * 100
                ax2.text(
                    xi,
                    ti * 1.1,
                    f"{pct:.0f}%",
                    ha="center",
                    va="bottom",
                    fontsize=7,
                    color="tab:green",
                )

        # X labels & legend
        ax1.set_xticks(x)
        ax1.set_xticklabels(stage_labels, rotation=45)
        lines = [b1, b2, *ln1, *ln2, *ln3, *ln4]
        labels = [
            "Proto RS",
            "Impr RS",
            "Proto Par",
            "Impr Par",
            "Proto Thrpt",
            "Impr Thrpt",
        ]
        ax1.legend(lines, labels, fontsize=8, loc="upper left")
        ax1.set_title(config, fontsize=10)

    # remove any empty axes
    for j in range(idx + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.suptitle(
        "Prototype vs Improved — Records Sent, Parallelism & Throughput",
        fontsize=16,
    )
    plt.subplots_adjust(top=0.92, hspace=0.4)
    plt.savefig(path, dpi=300, bbox_inches="tight")


improved = {
    "Configuration 1-16-1": [
        {"Records Received": 100_000, "Records Sent": 448_940, "Parallelism": 1},
        {"Records Received": 6811 + 45, "Records Sent": 6799 + 41, "Parallelism": 16},
        {"Records Received": 6840, "Records Sent": 6840, "Parallelism": 1},
    ],
    "Configuration 1-8-1": [
        {"Records Received": 100_000, "Records Sent": 448_940, "Parallelism": 1},
        {"Records Received": 6733 + 26, "Records Sent": 6727 + 24, "Parallelism": 8},
        {"Records Received": 6751, "Records Sent": 6751, "Parallelism": 1},
    ],
    "Configuration 1-6-1": [
        {"Records Received": 100_000, "Records Sent": 448_940, "Parallelism": 1},
        {"Records Received": 6719 + 12, "Records Sent": 6714 + 11, "Parallelism": 6},
        {"Records Received": 6725, "Records Sent": 6725, "Parallelism": 1},
    ],
    "Configuration 1-4-1": [
        {"Records Received": 100_000, "Records Sent": 448_940, "Parallelism": 1},
        {"Records Received": 6705 + 13, "Records Sent": 6701 + 12, "Parallelism": 4},
        {"Records Received": 6713, "Records Sent": 6713, "Parallelism": 1},
    ],
}

prototype = {
    "Configuration 1-16-1": [
        {"Records Received": 100_000, "Records Sent": 448_940, "Parallelism": 1},
        {"Records Received": 197, "Records Sent": 181, "Parallelism": 16},
        {"Records Received": 181, "Records Sent": 181, "Parallelism": 1},
    ],
    "Configuration 1-8-1": [
        {"Records Received": 100_000, "Records Sent": 448_940, "Parallelism": 1},
        {"Records Received": 106, "Records Sent": 98, "Parallelism": 8},
        {"Records Received": 98, "Records Sent": 98, "Parallelism": 1},
    ],
    "Configuration 1-6-1": [
        {"Records Received": 100_000, "Records Sent": 448_940, "Parallelism": 1},
        {"Records Received": 77, "Records Sent": 71, "Parallelism": 6},
        {"Records Received": 71, "Records Sent": 71, "Parallelism": 1},
    ],
    "Configuration 1-4-1": [
        {"Records Received": 100_000, "Records Sent": 448_940, "Parallelism": 1},
        {"Records Received": 51, "Records Sent": 47, "Parallelism": 4},
        {"Records Received": 47, "Records Sent": 47, "Parallelism": 1},
    ],
}


generate_image(
    improved,
    "improved.png",
)
generate_image(
    prototype,
    "prototype.png",
)
generate_comparison_image(prototype, improved, "comparison.png")
