package com.timecho.iotdb.service;

import org.apache.iotdb.db.service.DataNodeInternalRPCService;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService.Processor;

public class DataNodeInternalRPCServiceNew extends DataNodeInternalRPCService {

  @Override
  public void initTProcessor()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    impl = new DataNodeInternalRPCServiceImplNew();
    initSyncedServiceImpl(null);
    processor = new Processor<>(impl);
  }

  private static class DataNodeInternalRPCServiceHolder {
    private static final DataNodeInternalRPCServiceNew INSTANCE =
        new DataNodeInternalRPCServiceNew();

    private DataNodeInternalRPCServiceHolder() {}
  }

  public static DataNodeInternalRPCServiceNew getInstance() {
    return DataNodeInternalRPCServiceHolder.INSTANCE;
  }
}
