import random

import matplotlib.pyplot as plt

# Parameters for the U-shaped distribution
beta_alpha = 0.4
max_duration_ms = 10_000

# Generate samples (e.g. 10k points)
samples = [
    max(
        1,
        min(
            max_duration_ms,
            int(1 + random.betavariate(beta_alpha, beta_alpha) * (max_duration_ms - 1)),
        ),
    )
    for _ in range(10_000)
]

# Plot histogram: use, say, 100 bins over [1,10_000]
plt.figure()
plt.hist(samples, bins=100, density=True)
plt.xlabel("Duration (ms)")
plt.ylabel("Probability Density")
plt.title(
    f"U-shaped Distribution (Beta(a,a), a={beta_alpha}, up to {max_duration_ms} ms)"
)
plt.tight_layout()
plt.show()
