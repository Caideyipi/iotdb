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

package org.apache.iotdb.confignode.consensus.request.write.mlnode;

import org.apache.iotdb.common.rpc.thrift.TMLNodeConfiguration;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class UpdateMLNodePlan extends ConfigPhysicalPlan {

  private TMLNodeConfiguration mlNodeConfiguration;

  public UpdateMLNodePlan() {
    super(ConfigPhysicalPlanType.UpdateMLNodeConfiguration);
  }

  public UpdateMLNodePlan(TMLNodeConfiguration mlNodeConfiguration) {
    this();
    this.mlNodeConfiguration = mlNodeConfiguration;
  }

  public TMLNodeConfiguration getMlNodeConfiguration() {
    return mlNodeConfiguration;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ThriftCommonsSerDeUtils.serializeTMLNodeConfiguration(mlNodeConfiguration, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    mlNodeConfiguration = ThriftCommonsSerDeUtils.deserializeTMLNodeConfiguration(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!getType().equals(((UpdateMLNodePlan) o).getType())) {
      return false;
    }
    UpdateMLNodePlan that = (UpdateMLNodePlan) o;
    return mlNodeConfiguration.equals(that.mlNodeConfiguration);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), mlNodeConfiguration);
  }
}
