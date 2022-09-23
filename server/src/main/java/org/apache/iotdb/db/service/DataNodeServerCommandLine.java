/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.service;

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.ServerCommandLine;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataNodeServerCommandLine extends ServerCommandLine {

  private static final Logger logger = LoggerFactory.getLogger(DataNodeServerCommandLine.class);

  // join an established cluster
  private static final String MODE_START = "-s";
  // send a request to remove a node, more arguments: ip-of-removed-node
  // metaport-of-removed-node
  private static final String MODE_REMOVE = "-r";

  private static final String USAGE =
      "Usage: <-s|-r> "
          + "[-D{} <configure folder>] \n"
          + "-s: start the node to the cluster\n"
          + "-r: remove the node out of the cluster\n";

  @Override
  protected String getUsage() {
    return USAGE;
  }

  @Override
  protected int run(String[] args) throws Exception {
    if (args.length < 1) {
      usage(null);
      return -1;
    }

    DataNode dataNode = DataNode.getInstance();
    // check config of iotdb,and set some configs in cluster mode
    try {
      dataNode.serverCheckAndInit();
    } catch (ConfigurationException | IOException e) {
      logger.error("Meet error when doing start checking", e);
      return -1;
    }
    String mode = args[0];
    logger.info("Running mode {}", mode);

    // initialize the current node and its services
    if (!dataNode.initLocalEngines()) {
      logger.error("Init local engines error, stop process!");
      return -1;
    }

    // we start IoTDB kernel first. then we start the cluster module.
    if (MODE_START.equals(mode)) {
      dataNode.doAddNode();
    } else if (MODE_REMOVE.equals(mode)) {
      doRemoveNode(args);
    } else {
      logger.error("Unrecognized mode {}", mode);
    }
    return 0;
  }

  /**
   * remove datanodes from cluster
   *
   * @param args ids for removed datanodes, split with ','
   */
  private void doRemoveNode(String[] args) throws BadNodeUrlException, TException, IoTDBException {

    logger.info("Start to remove DataNode from cluster");
    if (args.length != 2) {
      // logger.info("Usage: <node-ids>, split with ':'");
      logger.info("Usage: <node-id>");
      return;
    }

    ConfigNodeInfo.getInstance().loadConfigNodeList();
    List<TDataNodeLocation> dataNodeLocations = buildDataNodeLocations(args[1]);
    if (dataNodeLocations.isEmpty()) {
      throw new BadNodeUrlException("No DataNode to remove");
    }
    //    logger.info(
    //        "There are DataNodes to be removed. size is: {}, detail: {}",
    //        dataNodeLocations.size(),
    //        dataNodeLocations);
    logger.info("Start to remove datanode, detail:{}", dataNodeLocations);
    TDataNodeRemoveReq removeReq = new TDataNodeRemoveReq(dataNodeLocations);
    try (ConfigNodeClient configNodeClient = new ConfigNodeClient()) {
      TDataNodeRemoveResp removeResp = configNodeClient.removeDataNode(removeReq);
      logger.info("Remove result {} ", removeResp.toString());
      if (removeResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IoTDBException(
            removeResp.getStatus().toString(), removeResp.getStatus().getCode());
      }
      logger.info(
          "Submit remove-datanode request successfully, "
              + "more details are shown in the logs of confignode-leader and removed-datanode, "
              + "and after the process of removing datanode is over, "
              + "you are supposed to delete directory and data of the removed-datanode manually");
    }
  }

  /**
   * fetch all datanode info from ConfigNode, then compare with input 'ids'
   *
   * @param ids datanode id, split with ':'
   * @return TDataNodeLocation list
   */
  private List<TDataNodeLocation> buildDataNodeLocations(String ids) {
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    if (ids == null || ids.trim().isEmpty()) {
      return dataNodeLocations;
    }

    // Currently support only single id, delete this line when supports multi ones
    if (ids.split(":").length > 1) {
      logger.info("Incorrect id format, usage: <node-id>");
      return dataNodeLocations;
    }

    try (ConfigNodeClient client = new ConfigNodeClient()) {
      for (String id : ids.split(":")) {
        List<TDataNodeLocation> NodeLocationResult =
            client.getDataNodeConfiguration(Integer.parseInt(id)).getDataNodeConfigurationMap()
                .values().stream()
                .map(TDataNodeConfiguration::getLocation)
                .collect(Collectors.toList());
        if (NodeLocationResult.isEmpty()) {
          logger.warn("DataNode {} is not in cluster, skipped...", id);
          continue;
        }
        if (!dataNodeLocations.contains(NodeLocationResult.get(0))) {
          dataNodeLocations.add(NodeLocationResult.get(0));
        }
      }
    } catch (TException e) {
      logger.error("Get data node locations failed", e);
    } catch (NumberFormatException e) {
      logger.info("Incorrect id format, usage: <node-ids>, split with ':'");
    }

    return dataNodeLocations;
  }
}
