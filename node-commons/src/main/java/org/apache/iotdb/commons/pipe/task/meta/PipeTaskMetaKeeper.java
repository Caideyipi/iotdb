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
package org.apache.iotdb.commons.pipe.task.meta;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class PipeTaskMetaKeeper {

  protected final List<PipeTaskMeta> pipeTaskMetas = new ArrayList<>();

  public PipeTaskMetaKeeper() {}

  public void addPipeTaskMeta(PipeTaskMeta pipeTaskMeta) {
    pipeTaskMetas.add(pipeTaskMeta);
  }

  public void removePipeTaskMeta(PipeTaskMeta pipeTaskMeta) {
    pipeTaskMetas.remove(pipeTaskMeta);
  }

  public List<PipeTaskMeta> getPipeTaskMetas() {
    return pipeTaskMetas;
  }

  public void clear() {
    this.pipeTaskMetas.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipeTaskMetaKeeper that = (PipeTaskMetaKeeper) o;
    return pipeTaskMetas.equals(that.pipeTaskMetas);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeTaskMetas);
  }
}
