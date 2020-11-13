#!/bin/sh
bin=`dirname "$0"`
basedir=`cd "$bin"/.. >/dev/null; pwd`

CONF_DIR=${basedir}/conf
LOG_DIR=${basedir}/logs

CLASSPATH=${CONF_DIR}:`find ${basedir}/lib -name "*jar"|xargs -i echo {}:|xargs echo|sed 's/ //g'`

JAVA_OPTS="-Dgraph.log.dir=${LOG_DIR} -Dgraph.log.file=graph.log -Dgraph.root.logger=INFO,RFA -Dgraph.security.logger=INFO,RFAS"

exec nohup java -Dgraph_home=${basedir} ${JAVA_OPTS}  -cp $CLASSPATH com.lightgraph.graph.server.Server 1>${LOG_DIR}/graph.log.out 2>&1 &