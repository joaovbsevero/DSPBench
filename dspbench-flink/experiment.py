import os
import time
import datetime

exec = "stream"

def change_prop(run, app, stage1=1, stage2=1, stage3=1, stage4=1, stage5=1, stage6=1, stage7=1, stage8=1, stage9=1, stage10=1, stage11=1, stage12=1, stage13=1):
    print("change prop")

def start_job(app):
    print("start job")
    # ./bin/dspbench-flink-cluster.sh /home/DSPBench/dspbench-flink/build/libs/dspbench-flink-uber-1.0.jar wordcount /home/DSPBench/dspbench-flink/build/resources/main/config/wordcount.properties
    os.system('./bin/dspbench-flink-cluster.sh ./build/libs/dspbench-flink-uber-1.0.jar ' + app + ' ./src/main/resources/config/' + app + '.properties')

def stop_cluster():
    os.system('~/maven/flink-1.18.1/bin/stop-cluster.sh')

def start_cluster():
    os.system('~/maven/flink-1.18.1/bin/start-cluster.sh')

def restart_cluster():
    print("restart cluster")
    stop_cluster()
    time.sleep(10)
    start_cluster()
    time.sleep(30)

def time_txt(app, conf, init_time, end_time):
    print("####################################################################")
    with open('./txts/'+app+'-'+exec+'-'+str(conf)+'.txt', 'a') as f:
        f.write(str(init_time) + " - " + str(end_time) + "\n")

start_cluster()

for i in range(1,6):
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("tweetslatency")
    end_time = datetime.datetime.now()
    time_txt("tweetslatency", 1136, init_time, end_time)
    restart_cluster()

stop_cluster()
