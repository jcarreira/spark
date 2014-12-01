if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters"
    exit 1
fi

SPARK_VERSION_NAME=spark-1.2.0-SNAPSHOT-bin

#sudo JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64/ ./make-distribution.sh --name $1 --tgz --with-tachyon -Pyarn -Phadoop-2.3 -Dhadoop.version=2.3.0 -DskipTests 
#
#if [ $? -ne 0 ]; then
#    echo "error with make-distribution"
#    exit 1
#fi

sudo cp $SPARK_VERSION_NAME-$1.tgz /usr/local/spark/
cd /usr/local/spark

for x in {1..16}; 
do nohup sudo scp $SPARK_VERSION_NAME-$1.tgz f$x:/usr/local/spark &
done
for x in {1..16}; 
do nohup sudo scp /usr/local/spark/install_joao.sh f$x:/usr/local/spark & 
done
sleep 3

ssh joao@fbox "dsh -g joao-cluster -c -- sudo /usr/local/spark/install_joao.sh $1"
ssh joao@fbox "dsh -g joao-cluster -c -- sudo chmod -R a+rwx /usr/local/spark/$SPARK_VERSION_NAME-$1"

sudo /usr/local/spark/$SPARK_VERSION_NAME-$1/sbin/stop-all.sh
sleep 1
sudo /usr/local/spark/$SPARK_VERSION_NAME-$1/sbin/start-all.sh
