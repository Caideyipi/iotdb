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
import os
import sys
import shutil

from iotdb.thrift.common.ttypes import TMLNodeLocation, TEndPoint

from iotdb.mlnode.client import client_manager
from iotdb.mlnode.config import descriptor
from iotdb.mlnode.constant import TSStatusCode
from iotdb.mlnode.log import logger
from iotdb.mlnode.service import MLNode

server: MLNode = None


def main():
    global server
    arguments = sys.argv
    if len(arguments) == 1:
        logger.info("Command line argument must be specified.")
        return
    command = arguments[1]
    if command == 'start':
        server = MLNode()
        server.start()
    elif command == 'remove':
        try:
            logger.info("Removing MLNode...")
            if len(arguments) >= 3:
                target_mlnode = arguments[2]
                # parameter pattern: <mlnode-id>/<ip>:<rpc-port>
                target_mlnode_id = int(target_mlnode.split('/')[0])
                target_rpc_address = target_mlnode.split('/')[1].split(':')[0]
                target_rpc_port = int(target_mlnode.split('/')[1].split(':')[1])
                logger.info('Got target MLNode id: {}, address: {}, port: {}'
                            .format(target_mlnode_id, target_rpc_address, target_rpc_port))
            else:
                target_mlnode_id = descriptor.get_config().get_mlnode_id()
                target_rpc_address = descriptor.get_config().get_mln_inference_rpc_address()
                target_rpc_port = descriptor.get_config().get_mln_inference_rpc_port()

            location = TMLNodeLocation(target_mlnode_id, TEndPoint(target_rpc_address, target_rpc_port))
            status = client_manager.borrow_config_node_client().node_remove(location)

            if status.code == TSStatusCode.SUCCESS_STATUS.get_status_code():
                logger.info('IoTDB-MLNode has successfully removed.')
                if os.path.exists(descriptor.get_config().get_mln_models_dir()):
                    shutil.rmtree(descriptor.get_config().get_mln_models_dir())

        except Exception as e:
            logger.error("Remove MLNode failed, because of: {}".format(e))
            sys.exit(1)
    else:
        logger.warning("Unknown argument: {}.".format(command))
