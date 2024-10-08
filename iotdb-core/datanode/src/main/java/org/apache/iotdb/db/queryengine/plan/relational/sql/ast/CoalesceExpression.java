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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class CoalesceExpression extends Expression {

  private final List<Expression> operands;

  public CoalesceExpression(Expression first, Expression second, Expression... additional) {
    this(ImmutableList.<Expression>builder().add(first, second).add(additional).build());
  }

  public CoalesceExpression(List<Expression> operands) {
    super(null);
    requireNonNull(operands, "operands is null");
    checkArgument(operands.size() >= 2, "must have at least two operands");

    this.operands = ImmutableList.copyOf(operands);
  }

  public CoalesceExpression(@Nonnull NodeLocation location, List<Expression> operands) {
    super(requireNonNull(location, "location is null"));
    requireNonNull(operands, "operands is null");
    checkArgument(operands.size() >= 2, "must have at least two operands");

    this.operands = ImmutableList.copyOf(operands);
  }

  public List<Expression> getOperands() {
    return operands;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCoalesceExpression(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return operands;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CoalesceExpression that = (CoalesceExpression) o;
    return Objects.equals(operands, that.operands);
  }

  @Override
  public int hashCode() {
    return operands.hashCode();
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }

  // =============== serialize =================
  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.COALESCE;
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(operands.size(), stream);
    for (Expression operand : operands) {
      serialize(operand, stream);
    }
  }

  public CoalesceExpression(ByteBuffer byteBuffer) {
    super(null);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    this.operands = new ArrayList<>(size);
    while (size-- > 0) {
      operands.add(deserialize(byteBuffer));
    }
  }
}
