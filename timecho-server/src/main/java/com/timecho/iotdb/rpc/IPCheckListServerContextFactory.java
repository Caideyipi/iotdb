package com.timecho.iotdb.rpc;

import org.apache.iotdb.external.api.thrift.JudgableServerContext;
import org.apache.iotdb.external.api.thrift.ServerContextFactory;

import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;

public class IPCheckListServerContextFactory implements ServerContextFactory {
  private static final Logger logger =
      LoggerFactory.getLogger(IPCheckListServerContextFactory.class);

  @Override
  public JudgableServerContext newServerContext(TProtocol out, Socket socket) {
    return new IPCheckListServerContext(socket);
  }
}
