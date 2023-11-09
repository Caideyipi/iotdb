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

# The defaulte venv environment is used if mln_interpreter_dir is not set. Please use absolute path without quotation mark
# mln_interpreter_dir=

# Set mln_force_reinstall to 1 to force reinstall MLNode
mln_force_reinstall=0
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

# fetch parameters with names
while getopts "i:r" opt; do
  case $opt in
    i) p_mln_interpreter_dir="$OPTARG"
    ;;
    r) p_mln_force_reinstall=1
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    exit 1
    ;;
  esac
done

if [ -z "$p_mln_interpreter_dir" ]; then
  echo "No interpreter_dir is set, use default value."
else
  mln_interpreter_dir="$p_mln_interpreter_dir"
fi

if [ -z "$p_mln_force_reinstall" ]; then
  echo "No check_version is set, use default value."
else
  mln_force_reinstall="$p_mln_force_reinstall"
fi
echo Script got inputs: "mln_interpreter_dir: $mln_interpreter_dir", "mln_force_reinstall: $mln_force_reinstall"

if [ -z $mln_interpreter_dir ]; then
  $(dirname "$0")/../venv/bin/python3 -c "import sys; print(sys.executable)" &&
    echo "Activate default venv environment" || (
    echo "Creating default venv environment" && python3 -m venv "$(dirname "$0")/../venv"
  )
  mln_interpreter_dir="$SCRIPT_DIR/../venv/bin/python3"
fi
echo "Calling venv to check: $mln_interpreter_dir"

# Change the working directory to the parent directory
cd "$SCRIPT_DIR/.."

echo "Confirming MLNode..."
$mln_interpreter_dir -m pip list | grep "apache-iotdb-mlnode" >/dev/null
if [ $? -eq 0 ]; then
  if [ $mln_force_reinstall -eq 0 ]; then
    echo "MLNode is already installed"
    exit 0
  fi
fi

echo "Installing MLNode..."
cd "$SCRIPT_DIR/../lib/"
for i in *.whl; do
  # if mln_force_reinstall is 1 then force reinstall MLNode
  if [ $mln_force_reinstall -eq 1 ]; then
    echo Force reinstall $i
    $mln_interpreter_dir -m pip install "$i" --force-reinstall -i https://pypi.tuna.tsinghua.edu.cn/simple --no-warn-script-location
  else
    $mln_interpreter_dir -m pip install "$i" -i https://pypi.tuna.tsinghua.edu.cn/simple --no-warn-script-location
  fi
  if [ $? -eq 0 ]; then
    echo "MLNode is installed successfully"
    exit 0
  fi
done

echo "Failed to install MLNode"
exit 1
