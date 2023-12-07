<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Apache IoTDB AINode

For developers, a runnable AINode can be obtained by compiling the source code using the following command:.

```shell
mvn clean package -DskipUTs -pl iotdb-core/ainode -am -P with-ainode
```
After the compilation is complete, you can find the packaged `iotdb-enterprise-ainode-xxx` file in the target folder under ainode. To start an AINode instance, run the `start-ainode.sh` script in the sbin folder. (Note: AINode can be successfully started and registered only if there is a running IoTDB cluster.)
```shell
bash sbin/start-ainode.sh
```
If you want to stop a running AINode, you can execute the `stop-ainode.sh` script. To remove AINode from the IoTDB cluster, run the `remove-ainode.sh` script.