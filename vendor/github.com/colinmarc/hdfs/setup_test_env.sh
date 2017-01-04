#!/bin/sh

HADOOP_DISTRO=${HADOOP_DISTRO-"cdh"}
HADOOP_HOME=${HADOOP_HOME-"/tmp/hadoop-$HADOOP_DISTRO"}
NN_PORT=${NN_PORT-"9000"}
HADOOP_NAMENODE="localhost:$NN_PORT"

if [ ! -d "$HADOOP_HOME" ]; then
  mkdir -p $HADOOP_HOME

  if [ $HADOOP_DISTRO = "cdh" ]; then
      HADOOP_URL="http://archive.cloudera.com/cdh5/cdh/5/hadoop-latest.tar.gz"
  elif [ $HADOOP_DISTRO = "hdp" ]; then
      HADOOP_URL="http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.4.0.0/tars/hadoop-2.7.1.2.4.0.0-169.tar.gz"
  else
      echo "No/bad HADOOP_DISTRO='${HADOOP_DISTRO}' specified"
      exit 1
  fi

  echo "Downloading Hadoop from $HADOOP_URL to ${HADOOP_HOME}/hadoop.tar.gz"
  curl -o ${HADOOP_HOME}/hadoop.tar.gz -L $HADOOP_URL

  echo "Extracting ${HADOOP_HOME}/hadoop.tar.gz into $HADOOP_HOME"
  tar zxf ${HADOOP_HOME}/hadoop.tar.gz --strip-components 1 -C $HADOOP_HOME
fi

MINICLUSTER_JAR=$(find $HADOOP_HOME -name "hadoop-mapreduce-client-jobclient*.jar" | grep -v tests | grep -v sources | head -1)
if [ ! -f "$MINICLUSTER_JAR" ]; then
  echo "Couldn't find minicluster jar"
  exit 1
fi
echo "minicluster jar found at $MINICLUSTER_JAR"


# start the namenode in the background
echo "Starting hadoop namenode..."
$HADOOP_HOME/bin/hadoop jar $MINICLUSTER_JAR minicluster -nnport $NN_PORT -datanodes 3 -nomr -format "$@" > minicluster.log 2>&1 &
sleep 30

HADOOP_FS="$HADOOP_HOME/bin/hadoop fs -Ddfs.block.size=1048576"
$HADOOP_FS -mkdir -p "hdfs://$HADOOP_NAMENODE/_test"
$HADOOP_FS -chmod 777 "hdfs://$HADOOP_NAMENODE/_test"

$HADOOP_FS -put ./test/foo.txt "hdfs://$HADOOP_NAMENODE/_test/foo.txt"
$HADOOP_FS -put ./test/mobydick.txt "hdfs://$HADOOP_NAMENODE/_test/mobydick.txt"
