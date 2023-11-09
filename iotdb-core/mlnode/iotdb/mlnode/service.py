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
import threading
import time
from datetime import datetime

import psutil
from iotdb.thrift.common.ttypes import TMLNodeConfiguration, TMLNodeLocation, TEndPoint, TNodeResource
from iotdb.thrift.confignode.ttypes import TNodeVersionInfo
from iotdb.thrift.mlnode import IMLNodeRPCService
from thrift.protocol import TCompactProtocol, TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

from iotdb.mlnode.client import client_manager
from iotdb.mlnode.config import descriptor
from iotdb.mlnode.constant import MLNODE_SYSTEM_FILE_NAME
from iotdb.mlnode.handler import MLNodeRPCServiceHandler
from iotdb.mlnode.log import logger


class RPCService(threading.Thread):
    def __init__(self):
        super().__init__()
        processor = IMLNodeRPCService.Processor(handler=MLNodeRPCServiceHandler())
        transport = TSocket.TServerSocket(host=descriptor.get_config().get_mln_inference_rpc_address(),
                                          port=descriptor.get_config().get_mln_inference_rpc_port())
        transport_factory = TTransport.TFramedTransportFactory()
        if descriptor.get_config().get_mln_thrift_compression_enabled():
            protocol_factory = TCompactProtocol.TCompactProtocolFactory()
        else:
            protocol_factory = TBinaryProtocol.TBinaryProtocolFactory()

        self.__pool_server = TServer.TThreadPoolServer(processor, transport, transport_factory, protocol_factory)

    def run(self) -> None:
        logger.info("The RPC service thread begin to run...")
        self.__pool_server.serve()


class MLNode(object):
    def __init__(self):
        self.__rpc_service = RPCService()

    def start(self) -> None:
        logger.info('IoTDB-MLNode is starting...')
        system_path = descriptor.get_config().get_mln_system_dir()
        system_properties_file = os.path.join(descriptor.get_config().get_mln_system_dir(), MLNODE_SYSTEM_FILE_NAME)
        if not os.path.exists(system_path):
            try:
                os.makedirs(system_path)
                os.chmod(system_path, 0o777)
            except PermissionError as e:
                logger.error(e)
                raise e

        if not os.path.exists(system_properties_file):
            # If the system.properties file does not exist, the MLNode will register to ConfigNode.
            try:
                logger.info('IoTDB-MLNode is registering to ConfigNode...')
                mlnode_id = client_manager.borrow_config_node_client().node_register(
                    descriptor.get_config().get_cluster_name(),
                    self._generate_configuration(),
                    self._generate_version_info())
                descriptor.get_config().set_mlnode_id(mlnode_id)
                system_properties = {
                    'mlnode_id': mlnode_id,
                    'cluster_name': descriptor.get_config().get_cluster_name(),
                    'iotdb_version': descriptor.get_config().get_version_info(),
                    'commit_id': descriptor.get_config().get_build_info(),
                    'mln_rpc_address': descriptor.get_config().get_mln_inference_rpc_address(),
                    'mln_rpc_port': descriptor.get_config().get_mln_inference_rpc_port(),
                    'config_node_list': descriptor.get_config().get_mln_target_config_node_list(),
                }
                with open(system_properties_file, 'w') as f:
                    f.write('#' + str(datetime.now()) + '\n')
                    for key, value in system_properties.items():
                        f.write(key + '=' + str(value) + '\n')

            except Exception as e:
                logger.error('IoTDB-MLNode failed to register to ConfigNode: {}'.format(e))
                sys.exit(1)
        else:
            # If the system.properties file does exist, the MLNode will just restart.
            try:
                logger.info('IoTDB-MLNode is restarting...')
                client_manager.borrow_config_node_client().node_restart(
                    descriptor.get_config().get_cluster_name(),
                    self._generate_configuration(),
                    self._generate_version_info())

            except Exception as e:
                logger.error('IoTDB-MLNode failed to restart: {}'.format(e))
                sys.exit(1)

        self.__rpc_service.start()

        # sleep 100ms for waiting the rpc server start.
        time.sleep(0.1)
        logger.info('IoTDB-MLNode has successfully started.')

    @staticmethod
    def _generate_configuration() -> TMLNodeConfiguration:
        location = TMLNodeLocation(descriptor.get_config().get_mlnode_id(),
                                   TEndPoint(descriptor.get_config().get_mln_inference_rpc_address(),
                                             descriptor.get_config().get_mln_inference_rpc_port()))
        resource = TNodeResource(
            int(psutil.cpu_count()),
            int(psutil.virtual_memory()[0])
        )

        return TMLNodeConfiguration(location, resource)

    @staticmethod
    def _generate_version_info() -> TNodeVersionInfo:
        return TNodeVersionInfo(descriptor.get_config().get_version_info(),
                                descriptor.get_config().get_build_info())
