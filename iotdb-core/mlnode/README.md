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

# Apache IoTDB MLNode

For developers, a runnable MLNode can be obtained by compiling the source code using the following command:.

```shell
mvn clean package -DskipUTs -pl iotdb-core/mlnode -am -P with-mlnode
```
After the compilation is complete, you can find the packaged `apche-iotdb-mlnode-xxx` file in the target folder under mlnode. To start an MLNode instance, run the `start-mlnode.sh` script in the sbin folder. (Note: MLNode can be successfully started and registered only if there is a running IoTDB cluster.)
```shell
bash sbin/start-mlnode.sh
```
If you want to stop a running MLNode, you can execute the `stop-mlnode.sh` script. To remove MLNode from the IoTDB cluster, run the `remove-mlnode.sh` script.