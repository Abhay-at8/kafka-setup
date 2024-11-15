#cloud-boothook
#!/bin/bash
apt update -y
apt install openjdk-17-jre-headless -y
mkdir -p /home/kafka/
cd /home/kafka/
git clone https://github.com/Abhay-at8/kafka-setup.git
chmod -R 777  /home/kafka/
nohup bash  /home/kafka/kafka-setup/setup.sh < /dev/null 2> /dev/null > 
chmod -R 777  /home/kafka/
