#!/bin/sh

dir=`dirname "$0"`
basedir=`cd "$dir" >/dev/null; pwd`
deployPath=$1

if [ "${deployPath}" = "" ]
then
  deployPath=${basedir}/deploy
fi

if [ ! -d ${deployPath} ]
then
  mkdir $deployPath
fi

for i in `seq 1 5`
do
  rm -rf ${deployPath}/graphdb${i}
  cp -r graphdb ${deployPath}/graphdb${i}
  sed -i "s/8888/888${i}/g" ${deployPath}/graphdb${i}/conf/graph.properties
  sed -i "s/9874/987${i}/g" ${deployPath}/graphdb${i}/conf/graph.properties
  sed -i "s/^graph.master.servers.*/graph.master.servers=127.0.0.1:8881,127.0.0.1:8882,127.0.0.1:8883/g" ${deployPath}/graphdb${i}/conf/graph.properties
done

ps -ef|grep `pwd`|grep -v grep|awk '{print $2}'|xargs -i -t kill -9 {}
for script in `find ${deployPath} -name "start.sh"`
do
 echo "execute ${script}"
 nohup bash ${script} 1>/dev/null 2>&1 &
done

printf "wait bootstrap"
while true
do
  ret=`curl -H 'Content-Type:application/json' localhost:9871/cluster/_cat 2>>/dev/null|grep 8885|wc -l`
  if [ "${ret}" -eq "1" ]
  then
    printf "\nboostrap successful, enjoy!\n"
    curl -H 'Content-Type:application/json' localhost:9871/cluster/_cat 2>>/dev/null
    break;
  fi
  printf .
  sleep 1
done
