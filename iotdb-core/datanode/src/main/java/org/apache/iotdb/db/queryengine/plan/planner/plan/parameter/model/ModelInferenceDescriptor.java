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

package org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.model;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.db.queryengine.execution.operator.window.mlnode.InferenceWindowParameter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ModelInferenceDescriptor {

  private final String modelName;
  private final TEndPoint targetMLNode;
  private final ModelInformation modelInformation;
  private List<String> outputColumnNames;
  private InferenceWindowParameter inferenceWindowParameter;

  public ModelInferenceDescriptor(
      String modelName, TEndPoint targetMLNode, ModelInformation modelInformation) {
    this.modelName = modelName;
    this.targetMLNode = targetMLNode;
    this.modelInformation = modelInformation;
  }

  private ModelInferenceDescriptor(ByteBuffer buffer) {
    this.modelName = ReadWriteIOUtils.readString(buffer);
    this.targetMLNode =
        new TEndPoint(ReadWriteIOUtils.readString(buffer), ReadWriteIOUtils.readInt(buffer));
    this.modelInformation = ModelInformation.deserialize(buffer);
    int outputColumnNamesSize = ReadWriteIOUtils.readInt(buffer);
    if (outputColumnNamesSize == 0) {
      this.outputColumnNames = null;
    } else {
      this.outputColumnNames = new ArrayList<>();
      for (int i = 0; i < outputColumnNamesSize; i++) {
        this.outputColumnNames.add(ReadWriteIOUtils.readString(buffer));
      }
    }
    boolean hasInferenceWindowParameter = ReadWriteIOUtils.readBool(buffer);
    if (hasInferenceWindowParameter) {
      this.inferenceWindowParameter = InferenceWindowParameter.deserialize(buffer);
    } else {
      this.inferenceWindowParameter = null;
    }
  }

  public void setInferenceWindowParameter(InferenceWindowParameter inferenceWindowParameter) {
    this.inferenceWindowParameter = inferenceWindowParameter;
  }

  public InferenceWindowParameter getInferenceWindowParameter() {
    return inferenceWindowParameter;
  }

  public ModelInformation getModelInformation() {
    return modelInformation;
  }

  public TEndPoint getTargetMLNode() {
    return targetMLNode;
  }

  public String getModelName() {
    return modelName;
  }

  public void setOutputColumnNames(List<String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  public List<String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(modelName, byteBuffer);
    ReadWriteIOUtils.write(targetMLNode.ip, byteBuffer);
    ReadWriteIOUtils.write(targetMLNode.port, byteBuffer);
    modelInformation.serialize(byteBuffer);
    if (outputColumnNames == null) {
      ReadWriteIOUtils.write(0, byteBuffer);
    } else {
      ReadWriteIOUtils.write(outputColumnNames.size(), byteBuffer);
      for (String outputColumnName : outputColumnNames) {
        ReadWriteIOUtils.write(outputColumnName, byteBuffer);
      }
    }
    if (inferenceWindowParameter == null) {
      ReadWriteIOUtils.write(false, byteBuffer);
    } else {
      ReadWriteIOUtils.write(true, byteBuffer);
      inferenceWindowParameter.serialize(byteBuffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(modelName, stream);
    ReadWriteIOUtils.write(targetMLNode.ip, stream);
    ReadWriteIOUtils.write(targetMLNode.port, stream);
    modelInformation.serialize(stream);
    if (outputColumnNames == null) {
      ReadWriteIOUtils.write(0, stream);
    } else {
      ReadWriteIOUtils.write(outputColumnNames.size(), stream);
      for (String outputColumnName : outputColumnNames) {
        ReadWriteIOUtils.write(outputColumnName, stream);
      }
    }
    if (inferenceWindowParameter == null) {
      ReadWriteIOUtils.write(false, stream);
    } else {
      ReadWriteIOUtils.write(true, stream);
      inferenceWindowParameter.serialize(stream);
    }
  }

  public static ModelInferenceDescriptor deserialize(ByteBuffer buffer) {
    return new ModelInferenceDescriptor(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ModelInferenceDescriptor that = (ModelInferenceDescriptor) o;
    return modelName.equals(that.modelName)
        && targetMLNode.equals(that.targetMLNode)
        && modelInformation.equals(that.modelInformation)
        && outputColumnNames.equals(that.outputColumnNames)
        && inferenceWindowParameter.equals(that.inferenceWindowParameter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        modelName, targetMLNode, modelInformation, outputColumnNames, inferenceWindowParameter);
  }
}
