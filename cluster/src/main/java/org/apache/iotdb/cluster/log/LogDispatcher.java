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

import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryResult;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.AppendNodeEntryHandler;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.NodeStatus;
import org.apache.iotdb.cluster.server.monitor.NodeStatusManager;
import org.apache.iotdb.cluster.server.monitor.Peer;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.TestOnly;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A LogDispatcher serves a raft leader by queuing logs that the leader wants to send to its
 * followers and send the logs in an ordered manner so that the followers will not wait for previous
 * logs for too long. For example: if the leader send 3 logs, log1, log2, log3, concurrently to
 * follower A, the actual reach order may be log3, log2, and log1. According to the protocol, log3
 * and log2 must halt until log1 reaches, as a result, the total delay may increase significantly.
 */
public class LogDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(LogDispatcher.class);
  RaftMember member;
  private static final ClusterConfig clusterConfig = ClusterDescriptor.getInstance().getConfig();
  protected boolean useBatchInLogCatchUp = clusterConfig.isUseBatchInLogCatchUp();
  Map<Node, BlockingQueue<SendLogRequest>> nodesLogQueues = new HashMap<>();
  Map<Node, Boolean> nodesEnabled = new HashMap<>();
  ExecutorService executorService;
  private static ExecutorService serializationService =
      IoTDBThreadPoolFactory.newFixedThreadPool(
          Runtime.getRuntime().availableProcessors(), "DispatcherEncoder");

  public static int bindingThreadNum = clusterConfig.getDispatcherBindingThreadNum();
  public static int maxBatchSize = 10;
  public static AtomicInteger concurrentSenderNum = new AtomicInteger();

  public LogDispatcher(RaftMember member) {
    this.member = member;
    executorService =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            bindingThreadNum * (member.getAllNodes().size() - 1),
            "LogDispatcher-" + member.getName());
    createQueueAndBindingThreads();
  }

  void createQueueAndBindingThreads() {
    for (Node node : member.getAllNodes()) {
      if (!ClusterUtils.isNodeEquals(node, member.getThisNode())) {
        nodesEnabled.put(node, true);
        nodesLogQueues.put(node, createQueueAndBindingThread(node));
      }
    }
  }

  @TestOnly
  public void close() throws InterruptedException {
    executorService.shutdownNow();
    executorService.awaitTermination(10, TimeUnit.SECONDS);
  }

  private ByteBuffer serializeTask(SendLogRequest request) {
    ByteBuffer byteBuffer = request.getVotingLog().getLog().serialize();
    request.getVotingLog().getLog().setByteSize(byteBuffer.capacity());
    return byteBuffer;
  }

  protected SendLogRequest transformRequest(Node node, SendLogRequest request) {
    return request;
  }

  public void offer(SendLogRequest request) {
    // do serialization here to avoid taking LogManager for too long
    if (!nodesLogQueues.isEmpty()) {
      SendLogRequest finalRequest = request;
      request.serializedLogFuture = serializationService.submit(() -> serializeTask(finalRequest));
    }

    long startTime = Statistic.LOG_DISPATCHER_LOG_ENQUEUE.getOperationStartTime();
    request.getVotingLog().getLog().setEnqueueTime(System.nanoTime());
    for (Entry<Node, BlockingQueue<SendLogRequest>> entry : nodesLogQueues.entrySet()) {
      boolean nodeEnabled = this.nodesEnabled.getOrDefault(entry.getKey(), false);
      if (!nodeEnabled) {
        continue;
      }

      request = transformRequest(entry.getKey(), request);

      BlockingQueue<SendLogRequest> nodeLogQueue = entry.getValue();
      try {
        boolean addSucceeded;
        if (ClusterDescriptor.getInstance().getConfig().isWaitForSlowNode()) {
          addSucceeded =
              nodeLogQueue.offer(
                  request,
                  ClusterDescriptor.getInstance().getConfig().getWriteOperationTimeoutMS(),
                  TimeUnit.MILLISECONDS);
        } else {
          addSucceeded = nodeLogQueue.add(request);
        }

        if (!addSucceeded) {
          logger.debug(
              "Log queue[{}] of {} is full, ignore the request to this node",
              entry.getKey(),
              member.getName());
        } else {
          request.setEnqueueTime(System.nanoTime());
        }
      } catch (IllegalStateException e) {
        logger.debug(
            "Log queue[{}] of {} is full, ignore the request to this node",
            entry.getKey(),
            member.getName());
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

  BlockingQueue<SendLogRequest> createQueueAndBindingThread(Node node) {
    BlockingQueue<SendLogRequest> logBlockingQueue;
    logBlockingQueue =
        new ArrayBlockingQueue<>(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    for (int i = 0; i < bindingThreadNum; i++) {
      executorService.submit(newDispatcherThread(node, logBlockingQueue));
    }
    return logBlockingQueue;
  }

  DispatcherThread newDispatcherThread(Node node, BlockingQueue<SendLogRequest> logBlockingQueue) {
    return new DispatcherThread(node, logBlockingQueue);
  }

  public static class SendLogRequest {

    private AppendNodeEntryHandler handler;
    private VotingLog votingLog;
    private AtomicBoolean leaderShipStale;
    private AtomicLong newLeaderTerm;
    private AppendEntryRequest appendEntryRequest;
    private long enqueueTime;
    private Future<ByteBuffer> serializedLogFuture;
    private int quorumSize;

    public SendLogRequest(
        VotingLog log,
        AtomicBoolean leaderShipStale,
        AtomicLong newLeaderTerm,
        AppendEntryRequest appendEntryRequest,
        int quorumSize) {
      this.setVotingLog(log);
      this.setLeaderShipStale(leaderShipStale);
      this.setNewLeaderTerm(newLeaderTerm);
      this.setAppendEntryRequest(appendEntryRequest);
      this.setQuorumSize(quorumSize);
    }

    public SendLogRequest(SendLogRequest request) {
      this.setVotingLog(request.votingLog);
      this.setLeaderShipStale(request.leaderShipStale);
      this.setNewLeaderTerm(request.newLeaderTerm);
      this.setAppendEntryRequest(request.appendEntryRequest);
      this.setQuorumSize(request.quorumSize);
      this.setEnqueueTime(request.enqueueTime);
      this.serializedLogFuture = request.serializedLogFuture;
    }

    public VotingLog getVotingLog() {
      return votingLog;
    }

    public void setVotingLog(VotingLog votingLog) {
      this.votingLog = votingLog;
    }

    public long getEnqueueTime() {
      return enqueueTime;
    }

    public void setEnqueueTime(long enqueueTime) {
      this.enqueueTime = enqueueTime;
    }

    public AtomicBoolean getLeaderShipStale() {
      return leaderShipStale;
    }

    public void setLeaderShipStale(AtomicBoolean leaderShipStale) {
      this.leaderShipStale = leaderShipStale;
    }

    public AtomicLong getNewLeaderTerm() {
      return newLeaderTerm;
    }

    void setNewLeaderTerm(AtomicLong newLeaderTerm) {
      this.newLeaderTerm = newLeaderTerm;
    }

    public AppendEntryRequest getAppendEntryRequest() {
      return appendEntryRequest;
    }

    public void setAppendEntryRequest(AppendEntryRequest appendEntryRequest) {
      this.appendEntryRequest = appendEntryRequest;
    }

    public int getQuorumSize() {
      return quorumSize;
    }

    public void setQuorumSize(int quorumSize) {
      this.quorumSize = quorumSize;
    }

    @Override
    public String toString() {
      return "SendLogRequest{" + "log=" + votingLog + '}';
    }
  }

  class DispatcherThread implements Runnable {

    Node receiver;
    private BlockingQueue<SendLogRequest> logBlockingDeque;
    protected List<SendLogRequest> currBatch = new ArrayList<>();
    private Peer peer;
    Client syncClient;
    AsyncClient asyncClient;
    private String baseName;

    DispatcherThread(Node receiver, BlockingQueue<SendLogRequest> logBlockingDeque) {
      this.receiver = receiver;
      this.logBlockingDeque = logBlockingDeque;
      this.peer = member.getPeer(receiver);
      if (!clusterConfig.isUseAsyncServer()) {
        syncClient = member.getSyncClient(receiver);
      }
      baseName = "LogDispatcher-" + member.getName() + "-" + receiver;
    }

    @Override
    public void run() {
      Thread.currentThread().setName(baseName);
      try {
        while (!Thread.interrupted()) {
          synchronized (logBlockingDeque) {
            SendLogRequest poll = logBlockingDeque.take();
            currBatch.add(poll);
            if (maxBatchSize > 1 && useBatchInLogCatchUp) {
              logBlockingDeque.drainTo(currBatch, maxBatchSize - 1);
            }
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Sending {} logs to {}", currBatch.size(), receiver);
          }
          Statistic.LOG_DISPATCHER_LOG_BATCH_SIZE.add(currBatch.size());
          serializeEntries();
          sendBatchLogs(currBatch);
          currBatch.clear();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.error("Unexpected error in log dispatcher", e);
      }
      logger.info("Dispatcher exits");
    }

    protected void serializeEntries() throws ExecutionException, InterruptedException {
      for (SendLogRequest request : currBatch) {
        Timer.Statistic.LOG_DISPATCHER_LOG_IN_QUEUE.calOperationCostTimeFromStart(
            request.getVotingLog().getLog().getEnqueueTime());
        Statistic.LOG_DISPATCHER_FROM_CREATE_TO_DEQUEUE.calOperationCostTimeFromStart(
            request.getVotingLog().getLog().getCreateTime());
        long start = Statistic.RAFT_SENDER_SERIALIZE_LOG.getOperationStartTime();
        request.getAppendEntryRequest().entry = request.serializedLogFuture.get();
        Statistic.RAFT_SENDER_SERIALIZE_LOG.calOperationCostTimeFromStart(start);
      }
    }

    private void appendEntriesAsync(
        List<ByteBuffer> logList, AppendEntriesRequest request, List<SendLogRequest> currBatch)
        throws TException {
      AsyncMethodCallback<AppendEntryResult> handler = new AppendEntriesHandler(currBatch);
      AsyncClient client = member.getSendLogAsyncClient(receiver);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: append entries {} with {} logs", member.getName(), receiver, logList.size());
      }
      if (client != null) {
        client.appendEntries(request, handler);
      }
    }

    private void appendEntriesSync(
        List<ByteBuffer> logList, AppendEntriesRequest request, List<SendLogRequest> currBatch) {

      long startTime = Timer.Statistic.RAFT_SENDER_WAIT_FOR_PREV_LOG.getOperationStartTime();
      if (!member.waitForPrevLog(peer, currBatch.get(0).getVotingLog().getLog())) {
        logger.warn(
            "{}: node {} timed out when appending {}",
            member.getName(),
            receiver,
            currBatch.get(0).getVotingLog());
        return;
      }
      Timer.Statistic.RAFT_SENDER_WAIT_FOR_PREV_LOG.calOperationCostTimeFromStart(startTime);

      if (syncClient == null) {
        syncClient = member.getSyncClient(receiver);
      }
      AsyncMethodCallback<AppendEntryResult> handler = new AppendEntriesHandler(currBatch);
      startTime = Timer.Statistic.RAFT_SENDER_SEND_LOG.getOperationStartTime();
      try {
        AppendEntryResult result = syncClient.appendEntries(request);
        Timer.Statistic.RAFT_SENDER_SEND_LOG.calOperationCostTimeFromStart(startTime);
        handler.onComplete(result);
      } catch (TException e) {
        syncClient.getInputProtocol().getTransport().close();
        ClientUtils.putBackSyncClient(syncClient);
        syncClient = member.getSyncClient(receiver);
        logger.warn("Failed logs: {}, first index: {}", logList, request.prevLogIndex + 1);
        handler.onError(e);
      }
    }

    protected AppendEntriesRequest prepareRequest(
        List<ByteBuffer> logList, List<SendLogRequest> currBatch, int firstIndex) {
      AppendEntriesRequest request = new AppendEntriesRequest();

      if (member.getHeader() != null) {
        request.setHeader(member.getHeader());
      }
      request.setLeader(member.getThisNode());
      request.setLeaderCommit(member.getLogManager().getCommitLogIndex());

      synchronized (member.getTerm()) {
        request.setTerm(member.getTerm().get());
      }

      request.setEntries(logList);
      // set index for raft
      request.setPrevLogIndex(
          currBatch.get(firstIndex).getVotingLog().getLog().getCurrLogIndex() - 1);
      try {
        request.setPrevLogTerm(currBatch.get(firstIndex).getAppendEntryRequest().prevLogTerm);
      } catch (Exception e) {
        logger.error("getTerm failed for newly append entries", e);
      }
      return request;
    }

    private void sendLogs(List<SendLogRequest> currBatch) throws TException {
      int logIndex = 0;
      logger.debug(
          "send logs from index {} to {}",
          currBatch.get(0).getVotingLog().getLog().getCurrLogIndex(),
          currBatch.get(currBatch.size() - 1).getVotingLog().getLog().getCurrLogIndex());
      while (logIndex < currBatch.size()) {
        long logSize = IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize();
        List<ByteBuffer> logList = new ArrayList<>();
        int prevIndex = logIndex;

        for (; logIndex < currBatch.size(); logIndex++) {
          long curSize = currBatch.get(logIndex).getAppendEntryRequest().entry.array().length;
          if (logSize - curSize <= IoTDBConstant.LEFT_SIZE_IN_REQUEST) {
            break;
          }
          logSize -= curSize;
          logList.add(currBatch.get(logIndex).getAppendEntryRequest().entry);
        }

        AppendEntriesRequest appendEntriesRequest = prepareRequest(logList, currBatch, prevIndex);
        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
          appendEntriesAsync(logList, appendEntriesRequest, currBatch.subList(prevIndex, logIndex));
        } else {
          appendEntriesSync(logList, appendEntriesRequest, currBatch.subList(prevIndex, logIndex));
        }
        for (; prevIndex < logIndex; prevIndex++) {
          Timer.Statistic.LOG_DISPATCHER_FROM_CREATE_TO_SENT.calOperationCostTimeFromStart(
              currBatch.get(prevIndex).getVotingLog().getLog().getCreateTime());
        }
      }
    }

    private void sendBatchLogs(List<SendLogRequest> currBatch) throws TException {
      if (currBatch.size() > 1) {
        if (useBatchInLogCatchUp) {
          sendLogs(currBatch);
        } else {
          for (SendLogRequest batch : currBatch) {
            Timer.Statistic.LOG_DISPATCHER_FROM_CREATE_TO_SENDING.calOperationCostTimeFromStart(
                batch.getVotingLog().getLog().getCreateTime());
            sendLog(batch);
          }
        }
      } else {
        sendLog(currBatch.get(0));
      }
    }

    void sendLogSync(SendLogRequest logRequest) {
      AppendNodeEntryHandler handler =
          member.getAppendNodeEntryHandler(
              logRequest.getVotingLog(),
              receiver,
              logRequest.leaderShipStale,
              logRequest.newLeaderTerm,
              logRequest.quorumSize);
      // TODO add async interface
      int retries = 5;
      try {
        long operationStartTime = Statistic.RAFT_SENDER_SEND_LOG.getOperationStartTime();
        for (int i = 0; i < retries; i++) {
          int concurrentSender = concurrentSenderNum.incrementAndGet();
          Statistic.RAFT_CONCURRENT_SENDER.add(concurrentSender);
          AppendEntryResult result = syncClient.appendEntry(logRequest.appendEntryRequest);
          concurrentSenderNum.decrementAndGet();
          if (result.status == Response.RESPONSE_OUT_OF_WINDOW) {
            Thread.sleep(100);
            Statistic.RAFT_SENDER_OOW.add(1);
          } else {
            long sendLogTime =
                Statistic.RAFT_SENDER_SEND_LOG.calOperationCostTimeFromStart(operationStartTime);
            NodeStatus nodeStatus = NodeStatusManager.getINSTANCE().getNodeStatus(receiver, false);
            nodeStatus.getSendEntryLatencySum().addAndGet(sendLogTime);
            nodeStatus.getSendEntryNum().incrementAndGet();

            long handleStart = Statistic.RAFT_SENDER_HANDLE_SEND_RESULT.getOperationStartTime();
            handler.onComplete(result);
            Statistic.RAFT_SENDER_HANDLE_SEND_RESULT.calOperationCostTimeFromStart(handleStart);
            break;
          }
        }
      } catch (TException e) {
        syncClient.getInputProtocol().getTransport().close();
        ClientUtils.putBackSyncClient(syncClient);
        syncClient = member.getSyncClient(receiver);
        handler.onError(e);
      } catch (Exception e) {
        handler.onError(e);
      }
    }

    private void sendLogAsync(SendLogRequest logRequest) {
      AppendNodeEntryHandler handler =
          member.getAppendNodeEntryHandler(
              logRequest.getVotingLog(),
              receiver,
              logRequest.leaderShipStale,
              logRequest.newLeaderTerm,
              logRequest.quorumSize);

      AsyncClient client = member.getAsyncClient(receiver);
      if (client != null) {
        try {
          client.appendEntry(logRequest.appendEntryRequest, handler);
        } catch (TException e) {
          handler.onError(e);
        }
      }
    }

    void sendLog(SendLogRequest logRequest) {
      Thread.currentThread()
          .setName(baseName + "-" + logRequest.getVotingLog().getLog().getCurrLogIndex());
      if (clusterConfig.isUseAsyncServer()) {
        sendLogAsync(logRequest);
      } else {
        sendLogSync(logRequest);
      }
      Timer.Statistic.LOG_DISPATCHER_FROM_CREATE_TO_SENT.calOperationCostTimeFromStart(
          logRequest.getVotingLog().getLog().getCreateTime());
    }

    class AppendEntriesHandler implements AsyncMethodCallback<AppendEntryResult> {

      private final List<AsyncMethodCallback<AppendEntryResult>> singleEntryHandlers;

      private AppendEntriesHandler(List<SendLogRequest> batch) {
        singleEntryHandlers = new ArrayList<>(batch.size());
        for (SendLogRequest sendLogRequest : batch) {
          AppendNodeEntryHandler handler =
              member.getAppendNodeEntryHandler(
                  sendLogRequest.getVotingLog(),
                  receiver,
                  sendLogRequest.getLeaderShipStale(),
                  sendLogRequest.getNewLeaderTerm(),
                  sendLogRequest.getQuorumSize());
          singleEntryHandlers.add(handler);
        }
      }

      @Override
      public void onComplete(AppendEntryResult aLong) {
        for (AsyncMethodCallback<AppendEntryResult> singleEntryHandler : singleEntryHandlers) {
          singleEntryHandler.onComplete(aLong);
        }
      }

      @Override
      public void onError(Exception e) {
        for (AsyncMethodCallback<AppendEntryResult> singleEntryHandler : singleEntryHandlers) {
          singleEntryHandler.onError(e);
        }
      }
    }
  }
}
