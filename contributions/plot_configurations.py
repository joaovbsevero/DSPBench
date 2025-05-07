import math

import matplotlib.pyplot as plt

screenshot_data = {
    "Configuration 16-16-16": [
        {"Records Received": 1, "Records Sent": 100000, "Parallelism": 1},
        {"Records Received": 99150, "Records Sent": 396279, "Parallelism": 16},
        {"Records Received": 91, "Records Sent": 75, "Parallelism": 16},
        {"Records Received": 74, "Records Sent": 74, "Parallelism": 16},
        {"Records Received": 74, "Records Sent": 0, "Parallelism": 1},
    ],
    "Configuration 1-16-1": [
        {"Records Received": 1, "Records Sent": 90199, "Parallelism": 1},
        {"Records Received": 74, "Records Sent": 58, "Parallelism": 16},
        {"Records Received": 58, "Records Sent": 0, "Parallelism": 1},
    ],
    "Configuration 8-8-8": [
        {"Records Received": 1, "Records Sent": 64413, "Parallelism": 1},
        {"Records Received": 49922, "Records Sent": 199514, "Parallelism": 8},
        {"Records Received": 42, "Records Sent": 34, "Parallelism": 8},
        {"Records Received": 34, "Records Sent": 34, "Parallelism": 8},
        {"Records Received": 33, "Records Sent": 0, "Parallelism": 1},
    ],
    "Configuration 1-8-1": [
        {"Records Received": 1, "Records Sent": 68334, "Parallelism": 1},
        {"Records Received": 42, "Records Sent": 34, "Parallelism": 8},
        {"Records Received": 34, "Records Sent": 0, "Parallelism": 1},
    ],
    "Configuration 6-6-6": [
        {"Records Received": 1, "Records Sent": 52351, "Parallelism": 1},
        {"Records Received": 37578, "Records Sent": 150316, "Parallelism": 6},
        {"Records Received": 29, "Records Sent": 23, "Parallelism": 6},
        {"Records Received": 23, "Records Sent": 23, "Parallelism": 6},
        {"Records Received": 23, "Records Sent": 0, "Parallelism": 1},
    ],
    "Configuration 1-6-1": [
        {"Records Received": 1, "Records Sent": 68323, "Parallelism": 1},
        {"Records Received": 29, "Records Sent": 23, "Parallelism": 6},
        {"Records Received": 23, "Records Sent": 0, "Parallelism": 1},
    ],
    "Configuration 4-4-4": [
        {"Records Received": 1, "Records Sent": 37626, "Parallelism": 1},
        {"Records Received": 25266, "Records Sent": 101124, "Parallelism": 4},
        {"Records Received": 26, "Records Sent": 22, "Parallelism": 4},
        {"Records Received": 22, "Records Sent": 22, "Parallelism": 4},
        {"Records Received": 22, "Records Sent": 0, "Parallelism": 1},
    ],
    "Configuration 1-4-": [
        {"Records Received": 1, "Records Sent": 57399, "Parallelism": 1},
        {"Records Received": 19, "Records Sent": 15, "Parallelism": 4},
        {"Records Received": 15, "Records Sent": 0, "Parallelism": 1},
    ],
}

# Filter to only include configs with 16, 8, or 6 in the name
filtered_data = {
    k: v for k, v in screenshot_data.items() if any(x in k for x in ["16", "8", "6"])
}

# Grid layout: 2 columns
configs = list(filtered_data.items())
num_configs = len(configs)
cols = 2
rows = math.ceil(num_configs / cols)

fig, axes = plt.subplots(nrows=rows, ncols=cols, figsize=(14, rows * 4))
axes = axes.flatten()
fig.tight_layout(pad=5)

for i, (config, stages) in enumerate(configs):
    ax = axes[i]
    stage_ids = [f"Stage {i}" for i in range(len(stages))]
    records_sent = [s["Records Sent"] for s in stages]
    parallelism = [s["Parallelism"] for s in stages]
    throughput = [
        s["Records Sent"] / s["Parallelism"] if s["Parallelism"] else 0 for s in stages
    ]

    ax.bar(stage_ids, records_sent, alpha=0.7, label="Records Sent")
    ax.plot(
        stage_ids,
        parallelism,
        color="orange",
        marker="o",
        label="Parallelism",
        linewidth=2,
    )
    ax.plot(
        stage_ids,
        throughput,
        color="green",
        marker="x",
        label="Throughput",
        linewidth=2,
    )

    ax.set_title(config, fontsize=10)
    ax.set_xlabel("Stages")
    ax.set_ylabel("Values")
    ax.tick_params(axis="x", rotation=45)
    ax.legend(fontsize=8)

# Remove unused subplots if any
for j in range(i + 1, len(axes)):
    fig.delaxes(axes[j])

plt.suptitle(
    "Flink Configs (16 / 8 / 6) – Records Sent, Parallelism, Throughput", fontsize=16
)
plt.subplots_adjust(top=0.92, hspace=0.4)
plt.savefig("screenshot_data.png", dpi=300, bbox_inches="tight")
