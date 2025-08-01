#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

echo -----------------------------------------------
echo Starting Settling the TsFile By Compaction
echo -----------------------------------------------

if [ -z "${IOTDB_HOME}" ]; then
  export IOTDB_HOME="$(cd "`dirname "$0"`"/../..; pwd)"
fi

if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

JVM_OPTS="-Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8"

CLASSPATH=""
for f in ${IOTDB_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

MAIN_CLASS=org.apache.iotdb.db.tools.settle.TsFileSettleByCompactionTool

"$JAVA" $JVM_OPTS -cp "$CLASSPATH" "$MAIN_CLASS" "$@"
exit $?
