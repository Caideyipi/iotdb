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

package org.apache.iotdb.commons.schema.filter.impl.singlechild;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class AbstractSingleChildFilter extends SchemaFilter {

  private final SchemaFilter child;

  protected AbstractSingleChildFilter(final SchemaFilter child) {
    // child should not be null
    this.child = child;
  }

  protected AbstractSingleChildFilter(final ByteBuffer byteBuffer) {
    this.child = SchemaFilter.deserialize(byteBuffer);
  }

  public SchemaFilter getChild() {
    return child;
  }

  @Override
  public void serialize(final ByteBuffer byteBuffer) {
    SchemaFilter.serialize(child, byteBuffer);
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    SchemaFilter.serialize(child, stream);
  }
}
