echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
source ~/.bashrc
mkdir -p /home/cloudlab/
#pip install -r req.txt 
nohup python3 sparkjob.py 2> test.log > test.log &