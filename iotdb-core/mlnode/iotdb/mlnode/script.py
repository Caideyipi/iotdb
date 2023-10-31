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
import shutil
import sys
from iotdb.mlnode.client import client_manager
from iotdb.mlnode.config import descriptor
from iotdb.mlnode.constant import TSStatusCode
from iotdb.mlnode.log import logger
from iotdb.mlnode.service import MLNode
from iotdb.thrift.common.ttypes import TMLNodeLocation, TEndPoint

server: MLNode = None


def main():
    global server
    arguments = sys.argv
    if len(arguments) == 1:
        logger.info("Command line argument must be specified.")
        return
    command = sys.argv[1]
    if command == 'start':
        server = MLNode()
        server.start()
    elif command == 'remove':
        try:
            logger.info("Remove MLNode...")
            location = TMLNodeLocation(descriptor.get_config().get_mlnode_id(),
                                       TEndPoint(descriptor.get_config().get_mln_inference_rpc_address(),
                                                 descriptor.get_config().get_mln_inference_rpc_port()))
            status = client_manager.borrow_config_node_client().node_remove(location)
            # remove the system properties

            if status.code == TSStatusCode.SUCCESS_STATUS.get_status_code():
                logger.info('IoTDB-MLNode has successfully removed.')
            logger.info("Remove MLNode successfully.")
        except Exception as e:
            logger.error("Remove MLNode failed, because of: {}".format(e))
            sys.exit(1)
    else:
        logger.warning("Unknown argument: {}.".format(command))