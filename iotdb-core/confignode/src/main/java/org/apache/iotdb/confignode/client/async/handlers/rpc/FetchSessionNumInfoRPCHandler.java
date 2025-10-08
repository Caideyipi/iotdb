package org.apache.iotdb.confignode.client.async.handlers.rpc;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSessionsNumInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class FetchSessionNumInfoRPCHandler
    extends DataNodeAsyncRequestRPCHandler<TFetchSessionsNumInfo> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(CheckTimeSeriesExistenceRPCHandler.class);

  public FetchSessionNumInfoRPCHandler(
      CnToDnAsyncRequestType requestType,
      int requestId,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      Map<Integer, TFetchSessionsNumInfo> responseMap,
      CountDownLatch countDownLatch) {
    super(requestType, requestId, targetDataNode, dataNodeLocationMap, responseMap, countDownLatch);
  }

  @Override
  public void onComplete(TFetchSessionsNumInfo response) {
    responseMap.put(requestId, response);
    nodeLocationMap.remove(requestId);
    countDownLatch.countDown();
  }

  @Override
  public void onError(Exception e) {
    String errorMsg =
        "Fetch sessions number info  error on DataNode: {id="
            + targetNode.getDataNodeId()
            + ", internalEndPoint="
            + targetNode.getInternalEndPoint()
            + "}"
            + e.getMessage();
    LOGGER.error(errorMsg);
    countDownLatch.countDown();
  }
}
