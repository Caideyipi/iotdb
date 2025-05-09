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

source "$(dirname "$0")/../conf/iotdb-common.sh"

checkAllConfigNodeVariables

LICENSE_PATH="$CONFIGNODE_HOME/activation/license"

if [ -f $LICENSE_PATH ]; then
    echo "WARNING: $LICENSE_PATH exists, which means this ConfigNode may have been activated."
    echo "WARNING: The following activation process will overwrite the current license file."
    sleep 1
    echo - - - - - - - - - -
    sleep 1
fi

SYSTEM_INFO_FILE_PATH="$CONFIGNODE_HOME/activation/system_info"

if [ -f $SYSTEM_INFO_FILE_PATH ]; then
    echo "Please copy the system_info's content and send it to Timecho: "
    cat $SYSTEM_INFO_FILE_PATH
    echo ""
else
    echo "$SYSTEM_INFO_FILE_PATH not exist, please start ConfigNode first to create this file."
    exit 1
fi
sleep 1

echo "Please enter license: "
read -r activation_code
echo "$activation_code" > $LICENSE_PATH
sleep 1
echo - - - - - - - - - -
sleep 1
echo "License has been successfully stored to $LICENSE_PATH"
echo "Import completed. Starting to verify the license..."
echo - - - - - - - - - -
main_class=com.timecho.iotdb.manager.activation.ActivationVerifier
iotdb_params="-DCONFIGNODE_CONF=${CONFIGNODE_CONF}"
iotdb_params="$iotdb_params -DCONFIGNODE_HOME=${CONFIGNODE_HOME}"

CLASSPATH=""
for f in "${CONFIGNODE_HOME}"/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

exec java $iotdb_params -cp "$CLASSPATH" "$main_class" $LICENSE_PATH
