import datetime
import os
import re
import time

exec = "stream"


def start_job(app):
    print("start job")
    # ./bin/dspbench-flink-cluster.sh /home/DSPBench/build/libs/dspbench-flink-uber-1.0.jar wordcount /home/DSPBench/build/resources/main/config/wordcount.properties
    os.system(
        "./bin/dspbench-flink-cluster.sh ./build/libs/dspbench-flink-uber-1.0.jar "
        + app
        + " ./src/main/resources/config/"
        + app
        + ".properties "
        + "2>&1 | tee /tmp/flink-submit.log"
    )


def stop_cluster():
    os.system("./flink-1.20.1/bin/stop-cluster.sh")


def start_cluster():
    os.system("./flink-1.20.1/bin/start-cluster.sh")


def restart_cluster():
    print("restart cluster")
    stop_cluster()
    time.sleep(5)
    start_cluster()
    time.sleep(5)


def update_path(dataset_path: str):
    with open("./src/main/resources/config/highprocessingtimevariance.properties") as f:
        file_content = f.read()

    matched = re.search(
        r"hptv.highprocessingtimevariance.source.path=(.*+)\n", file_content
    )
    if not matched:
        raise ValueError("Dataset path not found")

    previous = matched.group()
    replaced_content = file_content.replace(
        previous, f"hptv.highprocessingtimevariance.source.path={dataset_path}\n"
    )
    with open(
        "./src/main/resources/config/highprocessingtimevariance.properties", "w"
    ) as f:
        f.write(replaced_content)


# start_cluster()

for _ in range(1):
    restart_cluster()
    update_path("./data/highvariance.csv")

    init_time = datetime.datetime.now()
    start_job("highprocessingtimevariance")
    end_time = datetime.datetime.now()
