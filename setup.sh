
cd /home/kafka/

wget https://downloads.apache.org/kafka/3.8.1/kafka_2.12-3.8.1.tgz 
tar -xvf kafka_2.12-3.8.1.tgz
mv /home/kafka/kafka_2.12-3.8.1 /home/kafka/kafka

#chmod -R 777  /home/kafka/


mv /home/kafka/kafka-setup/server.properties /home/kafka/kafka/config/

mv /home/kafka/kafka-setup/kafka-server-start.sh  /home/kafka/kafka/bin/

nohup bash   /home/kafka/kafka-setup/startUp.sh < /dev/null 2> /dev/null > /dev/null &
