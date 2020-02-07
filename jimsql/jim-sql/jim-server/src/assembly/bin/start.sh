#!/bin/sh

BASEDIR=`dirname $0`/..
BASEDIR=`(cd "$BASEDIR"; pwd)`

# If a specific java binary isn't specified search for the standard 'java' binary
if [ -z "$JAVACMD" ] ; then
  if [ -n "$JAVA_HOME"  ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
      # IBM's JDK on AIX uses strange locations for the executables
      JAVACMD="$JAVA_HOME/jre/sh/java"
    else
      JAVACMD="$JAVA_HOME/bin/java"
    fi
  else
    JAVACMD=`which java`
  fi
fi

CLASSPATH="$BASEDIR"/conf/:"$BASEDIR"/lib/*
CONFIG_FILE="$BASEDIR/conf/jim.properties"
echo "$CLASSPATH"

if [ ! -x "$JAVACMD" ] ; then
  echo "Error: JAVA_HOME is not defined correctly."
  echo "  We cannot execute $JAVACMD"
  exit 1
fi


OPTS_MEMORY=`grep -ios 'opts.memory=.*$' ${CONFIG_FILE} | tr -d '\r'`
OPTS_MEMORY=${OPTS_MEMORY#*=}

#DEBUG_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5006"

nohup "$JAVACMD"\
  $OPTS_MEMORY $DEBUG_OPTS \
  -classpath "$CLASSPATH" \
  -Dbasedir="$BASEDIR" \
  -Dfile.encoding="UTF-8" \
  io.jimdb.sql.server.JimBootstrap \
  "$@" >/dev/null 2>/dev/null &
echo $! > jim.pid
