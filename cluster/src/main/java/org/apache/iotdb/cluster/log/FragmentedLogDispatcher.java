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

package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.logtypes.FragmentedLog;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class FragmentedLogDispatcher extends LogDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(FragmentedLogDispatcher.class);

  public FragmentedLogDispatcher(RaftMember member) {
    super(member);
  }

  public void offer(SendLogRequest request) {
    // do serialization here to avoid taking LogManager for too long

    long startTime = Statistic.LOG_DISPATCHER_LOG_ENQUEUE.getOperationStartTime();
    request.getVotingLog().getLog().setEnqueueTime(System.nanoTime());
    int i = 0;
    for (BlockingQueue<SendLogRequest> nodeLogQueue : nodesLogQueues.values()) {
      SendLogRequest fragmentedRequest = new SendLogRequest(request);
      fragmentedRequest.setVotingLog(new VotingLog(request.getVotingLog()));
      fragmentedRequest
          .getVotingLog()
          .setLog(new FragmentedLog((FragmentedLog) request.getVotingLog().getLog(), i++));
      try {
        boolean addSucceeded;
        if (ClusterDescriptor.getInstance().getConfig().isWaitForSlowNode()) {
          addSucceeded =
              nodeLogQueue.offer(
                  fragmentedRequest,
                  ClusterDescriptor.getInstance().getConfig().getWriteOperationTimeoutMS(),
                  TimeUnit.MILLISECONDS);
        } else {
          addSucceeded = nodeLogQueue.add(fragmentedRequest);
        }

        if (!addSucceeded) {
          logger.debug(
              "Log queue[{}] of {} is full, ignore the request to this node", i, member.getName());
        } else {
          request.setEnqueueTime(System.nanoTime());
        }
      } catch (IllegalStateException e) {
        logger.debug(
            "Log queue[{}] of {} is full, ignore the request to this node", i, member.getName());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    Statistic.LOG_DISPATCHER_LOG_ENQUEUE.calOperationCostTimeFromStart(startTime);

    if (Timer.ENABLE_INSTRUMENTING) {
      Statistic.LOG_DISPATCHER_FROM_CREATE_TO_ENQUEUE.calOperationCostTimeFromStart(
          request.getVotingLog().getLog().getCreateTime());
    }
  }

  LogDispatcher.DispatcherThread newDispatcherThread(
      Node node, BlockingQueue<SendLogRequest> logBlockingQueue) {
    return new DispatcherThread(node, logBlockingQueue);
  }

  class DispatcherThread extends LogDispatcher.DispatcherThread {

    DispatcherThread(Node receiver, BlockingQueue<SendLogRequest> logBlockingDeque) {
      super(receiver, logBlockingDeque);
    }

    @Override
    protected void serializeEntries() {
      for (SendLogRequest request : currBatch) {
        Timer.Statistic.LOG_DISPATCHER_LOG_IN_QUEUE.calOperationCostTimeFromStart(
            request.getVotingLog().getLog().getEnqueueTime());
        long start = Statistic.RAFT_SENDER_SERIALIZE_LOG.getOperationStartTime();
        request.getAppendEntryRequest().entry = request.getVotingLog().getLog().serialize();
        Statistic.RAFT_SENDER_SERIALIZE_LOG.calOperationCostTimeFromStart(start);
      }
    }
  }
}
