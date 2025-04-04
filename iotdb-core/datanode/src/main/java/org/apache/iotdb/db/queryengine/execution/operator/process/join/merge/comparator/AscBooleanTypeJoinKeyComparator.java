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

package org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.comparator;

import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Optional;

// Not sure about whether we need this type of Comparator
public class AscBooleanTypeJoinKeyComparator implements JoinKeyComparator {

  private static final AscBooleanTypeJoinKeyComparator INSTANCE =
      new AscBooleanTypeJoinKeyComparator();

  private AscBooleanTypeJoinKeyComparator() {}

  public static AscBooleanTypeJoinKeyComparator getInstance() {
    return INSTANCE;
  }

  @Override
  public Optional<Boolean> lessThan(
      TsBlock left,
      int leftColumnIndex,
      int leftRowIndex,
      TsBlock right,
      int rightColumnIndex,
      int rightRowIndex) {
    if (left.getColumn(leftColumnIndex).isNull(leftRowIndex)
        || right.getColumn(rightColumnIndex).isNull(rightRowIndex)) {
      return Optional.empty();
    }

    return Optional.of(
        transformBooleanToInt(left.getColumn(leftColumnIndex).getBoolean(leftRowIndex))
            < transformBooleanToInt(right.getColumn(rightColumnIndex).getBoolean(rightRowIndex)));
  }

  @Override
  public Optional<Boolean> equalsTo(
      TsBlock left,
      int leftColumnIndex,
      int leftRowIndex,
      TsBlock right,
      int rightColumnIndex,
      int rightRowIndex) {
    if (left.getColumn(leftColumnIndex).isNull(leftRowIndex)
        || right.getColumn(rightColumnIndex).isNull(rightRowIndex)) {
      return Optional.empty();
    }

    return Optional.of(
        left.getColumn(leftColumnIndex).getBoolean(leftRowIndex)
            == right.getColumn(rightColumnIndex).getBoolean(rightRowIndex));
  }

  @Override
  public Optional<Boolean> lessThanOrEqual(
      TsBlock left,
      int leftColumnIndex,
      int leftRowIndex,
      TsBlock right,
      int rightColumnIndex,
      int rightRowIndex) {
    if (left.getColumn(leftColumnIndex).isNull(leftRowIndex)
        || right.getColumn(rightColumnIndex).isNull(rightRowIndex)) {
      return Optional.empty();
    }

    return Optional.of(
        transformBooleanToInt(left.getColumn(leftColumnIndex).getBoolean(leftRowIndex))
            <= transformBooleanToInt(right.getColumn(rightColumnIndex).getBoolean(rightRowIndex)));
  }

  private int transformBooleanToInt(boolean value) {
    return value ? 1 : 0;
  }
}
