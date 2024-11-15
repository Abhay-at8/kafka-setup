inst_ip=$(curl http://checkip.amazonaws.com)
export inst_ip
INSTANCE_IP=$inst_ip
sed -i "s/{{INSTANCE_IP}}/$INSTANCE_IP/g" /home/kafka/kafka/config/server.properties
nohup /home/kafka/kafka/bin/zookeeper-server-start.sh /home/kafka/kafka/config/zookeeper.properties &
nohup /home/kafka/kafka/bin/kafka-server-start.sh /home/kafka/kafka/config/server.properties &
