import subprocess
from multiprocessing import Pool


def run_batch(batch: list[tuple[int, int]], threshold):
    cmds = []
    for config in batch:
        cmds.append(
            [
                "/home/severo/miniconda3/bin/python",
                "improved.py",
                "--fast-count",
                str(config[0]),
                "--slow-count",
                str(config[1]),
                "-t",
                str(threshold),
            ]
        )

    with Pool(processes=sum(batch[-1])) as p:
        p.map(subprocess.run, cmds)


if __name__ == "__main__":
    thresholds = [1000, 3000, 6000, 8000]

    config_batchs = [
        [
            (3, 1),
            (1, 3),
            (2, 2),
        ],
        [
            (4, 2),
            (2, 4),
            (3, 3),
        ],
        [
            (6, 2),
            (2, 6),
            (4, 4),
        ],
        [
            (8, 4),
            (4, 8),
            (6, 6),
        ],
        [
            (10, 6),
            (6, 10),
            (8, 8),
        ],
    ]

    for threshold in thresholds:
        for config_batch in config_batchs:
            run_batch(config_batch, threshold)
