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

source "$(dirname "$0")/iotdb-common.sh"

checkAllVariables

if [ -f "$IOTDB_HOME/activation/license" ]; then
    echo "$IOTDB_HOME/activation/license exists, which means this ConfigNode may have been activated."
    echo "Do you want to continue (the current license file will be overwritten) ? y/N"
    read -r continue_activation
    echo - - - - - - - - - -
    if [[ "$continue_activation" =~ ^[Yy]$ ]]; then
        echo "Continue activating..."
    else
        exit 0
    fi
fi

if [ -f "$IOTDB_HOME/activation/system_info" ]; then
    echo "Please copy the system_info's content and send it to Timecho: "
    cat "$IOTDB_HOME/activation/system_info"
    echo ""
else
    echo "$IOTDB_HOME/activation/system_info not exist, please start ConfigNode first to create this file."
    exit 1
fi

echo "Please enter license: "
read -r activation_code
echo - - - - - - - - - -
echo "$activation_code" > "$IOTDB_HOME/activation/license"
echo "License has been stored to $IOTDB_HOME/activation/license"

echo "Import completed. Please start cluster and execute 'show cluster' to verify activation status."
