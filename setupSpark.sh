echo $1
export KAFKA_HOST=$1
cd /home/kafka/kafka-setup

mv spark-3.5.3-bin-hadoop3 /opt/spark


nohup bash   /home/kafka/kafka-setup/sparkSetup.sh < /dev/null 2> /dev/null > /dev/null &