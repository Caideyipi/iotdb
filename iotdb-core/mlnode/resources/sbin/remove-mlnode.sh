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

echo ---------------------------
echo Removing IoTDB MLNode
echo ---------------------------

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "SCRIPT_DIR: $SCRIPT_DIR"
chmod u+x $(dirname "$0")/../conf/mlnode-env.sh
mln_interpreter_dir=$(sed -n 's/^mln_interpreter_dir=\(.*\)$/\1/p' $(dirname "$0")/../conf/mlnode-env.sh)
mln_system_dir=$(sed -n 's/^mln_system_dir=\(.*\)$/\1/p' $(dirname "$0")/../conf/iotdb-mlnode.properties)
bash $(dirname "$0")/../conf/mlnode-env.sh $*
if [ $? -eq 1 ]; then
    echo "Environment check failed. Exiting..."
    exit 1
fi

# fetch parameters with names
while getopts "i:t:rn" opt; do
  case $opt in
    i) p_mln_interpreter_dir="$OPTARG"
    ;;
    r) p_mln_force_reinstall="$OPTARG"
    ;;
    t) p_mln_remove_target="$OPTARG"
    ;;
    n)
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    exit 1
    ;;
  esac
done

# If mln_interpreter_dir in parameters is empty:
if [ -z "$p_mln_interpreter_dir" ]; then
  # If mln_interpreter_dir in ../conf/mlnode-env.sh is empty, set default value to ../venv/bin/python3
  if [ -z "$mln_interpreter_dir" ]; then
    mln_interpreter_dir="$SCRIPT_DIR/../venv/bin/python3"
  fi
else
  # If mln_interpreter_dir in parameters is not empty, set mln_interpreter_dir to the value in parameters
  mln_interpreter_dir="$p_mln_interpreter_dir"
fi

# If mln_system_dir is empty, set default value to ../data/mlnode/system
if [ -z "$mln_system_dir" ]
then
  mln_system_dir="$SCRIPT_DIR/../data/mlnode/system"
fi

echo "Script got parameters: mln_interpreter_dir: $mln_interpreter_dir, mln_system_dir: $mln_system_dir"

# check if mln_interpreter_dir is an absolute path
if [[ "$mln_interpreter_dir" != /* ]]; then
    mln_interpreter_dir="$SCRIPT_DIR/$mln_interpreter_dir"
fi

# Change the working directory to the parent directory
cd "$SCRIPT_DIR/.."
mln_mlnode_dir=$(dirname "$mln_interpreter_dir")/mlnode


if [ -z "$p_mln_remove_target" ]; then
  echo No target MLNode set, use system.properties
  $mln_mlnode_dir remove
else
  $mln_mlnode_dir remove $p_mln_remove_target
fi

if [ $? -eq 1 ]; then
    echo "Remove MLNode failed. Exiting..."
    exit 1
fi

bash $SCRIPT_DIR/stop-mlnode.sh $*

# Remove system directory
rm -rf $mln_system_dir