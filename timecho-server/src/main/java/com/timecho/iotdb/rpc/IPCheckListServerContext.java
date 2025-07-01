package com.timecho.iotdb.rpc;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.external.api.thrift.JudgableServerContext;

import java.io.IOException;
import java.net.Socket;

public class IPCheckListServerContext implements JudgableServerContext {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  Socket client;

  public IPCheckListServerContext(Socket socket) {
    this.client = socket;
  }

  @Override
  public boolean whenConnect() {
    if (config.isEnableWhiteList() || config.isEnableBlackList()) {
      String clientIP = client.getInetAddress().getHostAddress();
      // check black list and white list
      if (IPFilter.isDeniedConnect(clientIP)) {
        try {
          client.close();
          return false;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return true;
  }

  @Override
  public boolean whenDisconnect() {
    return true;
  }

  @Override
  public <T> T unwrap(Class<T> iface) {
    return JudgableServerContext.super.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return JudgableServerContext.super.isWrapperFor(iface);
  }
}
