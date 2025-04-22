#!/usr/bin/env python3
"""
Script: generate_synthetic_flink_dataset.py

Generates a CSV dataset to stress-test Apache Flink's load balancer by simulating
web search events with high-variance and U-shaped processing times per event.

Each line in the output CSV has the format:
ID, Name, Content, duration1, duration2, ..., durationN

Where:
- ID: sequential integer identifier for the event
- Name: event-specific name (e.g., "user<ID>")
- Content: synthetic content (e.g., "synthetic content <ID>")
- durationX: integer seconds for each synthetic web search

This script defaults to:
  * 0–50 searches per event
  * Durations between 1–120 seconds

Supports multiple distributions for durations:
  * uniform: durations between 1 and max_duration
  * pareto: heavy-tailed via random.paretovariate(alpha) * scale (clamped)
  * normal: gaussian sample with mean mu and stddev sigma (clamped)
  * ushape: U-shaped Beta(a,a) distribution with parameter beta_alpha (clamped)

Usage:
    python generate_synthetic_flink_dataset.py \
        --num-events 1000 \
        --distribution ushape --beta-alpha 0.3 \
        --output dataset.csv
"""

import argparse
import csv
import random


def generate_dataset(
    num_events,
    min_searches,
    max_searches,
    distribution,
    max_duration,
    alpha,
    scale,
    mu,
    sigma,
    beta_alpha,
    output_file,
):
    """
    Generate the synthetic CSV dataset.
    """
    with open(output_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        for event_id in range(1, num_events + 1):
            # Number of synthetic web searches for this event
            num_searches = random.randint(min_searches, max_searches)

            # Generate durations based on chosen distribution
            durations = []
            for _ in range(num_searches):
                if distribution == "uniform":
                    d = random.randint(1, max_duration)
                elif distribution == "pareto":
                    raw = int(random.paretovariate(alpha) * scale)
                    d = max(1, min(max_duration, raw))
                elif distribution == "normal":
                    raw = int(random.gauss(mu, sigma))
                    d = max(1, min(max_duration, raw))
                elif distribution == "ushape":
                    # Beta(a,a) yields U-shape for a < 1
                    raw = random.betavariate(beta_alpha, beta_alpha)
                    d = max(1, min(max_duration, int(1 + raw * (max_duration - 1))))
                else:
                    raise ValueError(f"Unsupported distribution: {distribution}")
                durations.append(d)

            # Write CSV row: ID, Name, Content, durations...
            writer.writerow([event_id, " ".join(map(str, durations))])


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate synthetic Flink load-balancer test dataset"
    )
    parser.add_argument(
        "--num-events",
        type=int,
        default=1000,
        help="Total number of events to generate",
    )
    parser.add_argument(
        "--min-searches",
        type=int,
        default=0,
        help="Minimum number of searches per event",
    )
    parser.add_argument(
        "--max-searches",
        type=int,
        default=50,
        help="Maximum number of searches per event",
    )
    parser.add_argument(
        "--distribution",
        choices=["uniform", "pareto", "normal", "ushape"],
        default="pareto",
        help="Distribution for durations",
    )
    parser.add_argument(
        "--max-duration",
        type=int,
        default=120,
        help="Max seconds for any generated duration",
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=2.0,
        help="Alpha parameter for Pareto distribution",
    )
    parser.add_argument(
        "--scale", type=float, default=10.0, help="Scale factor for Pareto distribution"
    )
    parser.add_argument(
        "--mu", type=float, default=60.0, help="Mean for normal distribution"
    )
    parser.add_argument(
        "--sigma",
        type=float,
        default=30.0,
        help="Standard deviation for normal distribution",
    )
    parser.add_argument(
        "--beta-alpha",
        type=float,
        default=0.5,
        help="Alpha parameter for U-shape Beta distribution (a < 1 yields strong U)",
    )
    parser.add_argument(
        "--output", type=str, default="dataset.csv", help="Output CSV filename"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    generate_dataset(
        num_events=args.num_events,
        min_searches=args.min_searches,
        max_searches=args.max_searches,
        distribution=args.distribution,
        max_duration=args.max_duration,
        alpha=args.alpha,
        scale=args.scale,
        mu=args.mu,
        sigma=args.sigma,
        beta_alpha=args.beta_alpha,
        output_file=args.output,
    )
    print(
        f"Dataset generated: {args.output} ({args.num_events} events, up to {args.max_searches} searches each, distribution={args.distribution})"
    )
