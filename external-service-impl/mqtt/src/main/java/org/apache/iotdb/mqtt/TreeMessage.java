/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.mqtt;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

/** Message parsing into a tree */
public class TreeMessage extends Message {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TreeMessage.class);
  private String device;
  private List<String> measurements;
  private List<TSDataType> dataTypes;
  private List<String> values;

  public String getDevice() {
    return device;
  }

  public void setDevice(String device) {
    this.device = device;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public void setMeasurements(List<String> measurements) {
    this.measurements = measurements;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public List<String> getValues() {
    return values;
  }

  public void setValues(List<String> values) {
    this.values = values;
  }

  @Override
  public String toString() {
    return "Message{"
        + "device='"
        + device
        + '\''
        + ", timestamp="
        + super.timestamp
        + ", measurements="
        + measurements
        + ", values="
        + values
        + '}';
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    // Add parent class fields
    size += RamUsageEstimator.sizeOf(timestamp);
    // Add String field
    size += RamUsageEstimator.sizeOf(device);
    // Add List fields - includes list overhead and elements
    size += RamUsageEstimator.sizeOfCollection(measurements);
    size += RamUsageEstimator.sizeOfCollection(dataTypes);
    size += RamUsageEstimator.sizeOfCollection(values);
    return size;
  }
}
