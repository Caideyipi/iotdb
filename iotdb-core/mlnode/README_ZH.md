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

对于开发者来说，可以通过编译源码的方式获得可运行的MLNode，使用以下的指令进行编译：
```shell
mvn clean package -DskipUTs -pl iotdb-core/mlnode -am -P with-mlnode
```
完成编译后，可以在mlnode下的target文件夹下找到打包后的 `apche-iotdb-mlnode-xxx`文件，运行sbin文件夹下的`start-mlnode.sh`即可启动一个MLNode实例。（MLNode成功启动并注册的前提是已有一个运行中的IoTDB集群）

```shell
bash sbin/start-mlnode.sh
```
如果想要停止正在运行中的MLNode，可以运行`stop-mlnode.sh`脚本。如果要将MLNode移除出IoTDB集群，则运行`remove-mlnode.sh`脚本。