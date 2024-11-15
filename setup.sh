#cloud-boothook
#!/bin/bash
apt update -y
apt install openjdk-17-jre-headless -y
mkdir -p /home/kafka/
cd /home/rp/
git clone https://github.com/Abhay-at8/kafka-setup.git

wget https://downloads.apache.org/kafka/3.8.1/kafka_2.12-3.8.1.tgz
tar -xvf kafka_2.12-3.8.1.tgz
mv /home/kafka/kafka_2.12-3.8.1 /home/kafka/kafka

chmod -R 777  /home/kafka/


mv /home/kafka/kafka-setup/server.properties /home/kafka/kafka/config/

mv /home/kafka/kafka-setup/kafka-server-start.sh  /home/kafka/kafka/bin/

nohup bash  startUp.sh