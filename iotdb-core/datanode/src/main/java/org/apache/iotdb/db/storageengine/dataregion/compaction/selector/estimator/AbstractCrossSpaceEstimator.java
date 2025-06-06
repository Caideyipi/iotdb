/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator;

import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.read.TsFileSequenceReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Estimate the memory cost of one cross space compaction task with specific source files based on
 * its corresponding implementation.
 */
public abstract class AbstractCrossSpaceEstimator extends AbstractCompactionEstimator {

  @Override
  protected TsFileSequenceReader getReader(String filePath) throws IOException {
    return new CompactionTsFileReader(filePath, CompactionType.CROSS_COMPACTION);
  }

  public long estimateCrossCompactionMemory(
      List<TsFileResource> seqResources, List<TsFileResource> unseqResources) throws IOException {
    List<TsFileResource> resources = new ArrayList<>(seqResources.size() + unseqResources.size());
    resources.addAll(seqResources);
    resources.addAll(unseqResources);
    CompactionEstimateUtils.addReadLock(resources);

    long cost = 0;
    try {
      CompactionTaskInfo taskInfo = calculatingCompactionTaskInfo(resources);
      cost += calculatingMetadataMemoryCost(taskInfo);
      cost += calculatingDataMemoryCost(taskInfo);
    } finally {
      CompactionEstimateUtils.releaseReadLock(resources);
    }
    return cost;
  }

  public abstract long roughEstimateCrossCompactionMemory(
      CompactionScheduleContext context,
      List<TsFileResource> seqResources,
      List<TsFileResource> unseqResources)
      throws IOException;
}
