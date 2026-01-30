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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RenameTimeSeriesProcedureTest {

  @Test
  public void serializeDeserializeTest() throws IOException, IllegalPathException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    PartialPath oldPath = new PartialPath("root.sg1.d1.s1");
    PartialPath newPath = new PartialPath("root.sg1.d1.s1_alias");
    RenameTimeSeriesProcedure proc =
        new RenameTimeSeriesProcedure("testQueryId", oldPath, newPath, false);

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      RenameTimeSeriesProcedure proc2 =
          (RenameTimeSeriesProcedure) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
      assertEquals(proc.getQueryId(), proc2.getQueryId());
      assertEquals(proc.getOldPath(), proc2.getOldPath());
      assertEquals(proc.getNewPath(), proc2.getNewPath());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
