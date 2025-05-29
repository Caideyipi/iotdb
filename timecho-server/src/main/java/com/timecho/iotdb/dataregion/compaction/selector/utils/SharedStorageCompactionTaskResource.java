package com.timecho.iotdb.dataregion.compaction.selector.utils;

import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.util.ArrayList;
import java.util.List;

public class SharedStorageCompactionTaskResource extends CrossCompactionTaskResource {
  private final List<TsFileResource> seqFiles;
  private final List<TsFileResource> unseqFiles;
  private final long maxVersion;

  public SharedStorageCompactionTaskResource(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles, long maxVersion) {
    this.seqFiles = new ArrayList<>(seqFiles);
    this.unseqFiles = new ArrayList<>(unseqFiles);
    this.maxVersion = maxVersion;
  }

  @Override
  public List<TsFileResource> getSeqFiles() {
    return seqFiles;
  }

  @Override
  public List<TsFileResource> getUnseqFiles() {
    return unseqFiles;
  }

  public long getMaxVersion() {
    return maxVersion;
  }
}
