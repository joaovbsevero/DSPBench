import datetime
import os
import time

exec = "stream"


def change_prop(
    run,
    app,
    stage1=1,
    stage2=1,
    stage3=1,
    stage4=1,
    stage5=1,
    stage6=1,
    stage7=1,
    stage8=1,
    stage9=1,
    stage10=1,
    stage11=1,
    stage12=1,
    stage13=1,
):
    if app == "adanalytics":
        # Remove last line
        os.system("sed -i '$ d' ./src/main/resources/config/adanalytics.properties")
        os.system("sed -i '$ d' ./src/main/resources/config/adanalytics.properties")
        os.system("sed -i '$ d' ./src/main/resources/config/adanalytics.properties")
        os.system("sed -i '$ d' ./src/main/resources/config/adanalytics.properties")
        os.system("sed -i '$ d' ./src/main/resources/config/adanalytics.properties")
        # Add lines
        os.system(
            'echo "aa.click.parser.threads='
            + str(stage1)
            + '" >> ./src/main/resources/config/adanalytics.properties'
        )
        os.system(
            'echo "aa.impressions.parser.threads='
            + str(stage2)
            + '" >> ./src/main/resources/config/adanalytics.properties'
        )
        os.system(
            'echo "aa.ctr.threads='
            + str(stage3)
            + '" >> ./src/main/resources/config/adanalytics.properties'
        )
        os.system(
            'echo "aa.sink.threads='
            + str(stage4)
            + '" >> ./src/main/resources/config/adanalytics.properties'
        )
        os.system(
            'echo "metrics.output=./metrics/'
            + exec
            + "/AA"
            + str(stage1)
            + str(stage2)
            + str(stage3)
            + str(stage4)
            + "/"
            + str(run)
            + '/" >> ./src/main/resources/config/adanalytics.properties'
        )


def start_job(app):
    print("start job")
    # ./bin/dspbench-flink-cluster.sh /home/DSPBench/build/libs/dspbench-flink-uber-1.0.jar wordcount /home/DSPBench/build/resources/main/config/wordcount.properties
    os.system(
        "./bin/dspbench-flink-cluster.sh ./build/libs/dspbench-flink-uber-1.0.jar "
        + app
        + " ./src/main/resources/config/"
        + app
        + ".properties"
    )


def stop_cluster():
    os.system("../flink-1.20.1/bin/stop-cluster.sh")


def start_cluster():
    os.system("../flink-1.20.1/bin/start-cluster.sh")


def restart_cluster():
    print("restart cluster")
    stop_cluster()  # os.system('~/flink-1.17.1/bin/stop-cluster.sh')
    time.sleep(10)
    start_cluster()  # os.system('~/flink-1.17.1/bin/start-cluster.sh')
    time.sleep(30)


def time_txt(app, conf, init_time, end_time):
    print("####################################################################")
    with open("./txts/" + app + "-" + exec + "-" + str(conf) + ".txt", "w") as f:
        f.write(str(init_time) + " - " + str(end_time) + "\n")


start_cluster()

for i in range(1, 2):
    # Change Confs on .properties
    change_prop(i, "adanalytics", 1, 1, 1, 1)
    restart_cluster()
    # Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("tweetslatency")
    end_time = datetime.datetime.now()
    time_txt("adanalytics", 1111, init_time, end_time)

stop_cluster()
