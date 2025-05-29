package com.timecho.iotdb.os.utils;

import org.apache.tsfile.fileSystem.FSType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RemoteStorageBlock {
  private final FSType fsType;
  private final String path;

  public RemoteStorageBlock(FSType fsType, String path) {
    this.fsType = fsType;
    this.path = path;
  }

  public FSType getFsType() {
    return fsType;
  }

  public String getPath() {
    return path;
  }

  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(fsType.toString(), stream);
    ReadWriteIOUtils.write(path, stream);
  }

  public static RemoteStorageBlock deserializeFrom(InputStream stream) throws IOException {
    FSType type = FSType.valueOf(ReadWriteIOUtils.readString(stream));
    String path = ReadWriteIOUtils.readString(stream);
    return new RemoteStorageBlock(type, path);
  }
}
