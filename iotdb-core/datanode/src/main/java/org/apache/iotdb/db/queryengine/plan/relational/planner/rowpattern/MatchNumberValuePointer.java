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

package org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class MatchNumberValuePointer implements ValuePointer {
  @Override
  public int hashCode() {
    return MatchNumberValuePointer.class.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof MatchNumberValuePointer;
  }

  public static void serialize(MatchNumberValuePointer pointer, ByteBuffer byteBuffer) {}

  public static void serialize(MatchNumberValuePointer pointer, DataOutputStream stream)
      throws IOException {}

  public static MatchNumberValuePointer deserialize(ByteBuffer byteBuffer) {
    return new MatchNumberValuePointer();
  }
}
