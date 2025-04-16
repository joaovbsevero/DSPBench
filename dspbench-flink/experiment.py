import datetime
import os
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
    time.sleep(10)
    start_cluster()
    time.sleep(30)


def time_txt(app, conf, init_time, end_time):
    print("####################################################################")
    with open("./txts/" + app + "-" + exec + "-" + str(conf) + ".txt", "w") as f:
        f.write(str(init_time) + " - " + str(end_time) + "\n")


for i in range(1, 2):
    # restart_cluster()

    init_time = datetime.datetime.now()
    start_job("tweetslatency")
    end_time = datetime.datetime.now()

    time_txt("tweetslatency", 1111, init_time, end_time)

# stop_cluster()
