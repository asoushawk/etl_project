apt-get update -y
apt-get install software-properties-common -y

add-apt-repository ppa:deadsnakes/ppa -y
apt-get update -y

apt-get install python3.8 -y
apt-get install python3.8-distutils -y
apt-get install pip -y

#apt-get install default-jdk -y
apt-get install openjdk-8-jdk -y

python3.8 -m pip install flask
python3.8 -m pip install pyspark==3.3.2
#export PYSPARK_PYTHON=/usr/bin/python3.8
#export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.8
python3.8 /home/main.py