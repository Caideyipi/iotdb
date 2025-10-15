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

package com.timecho.iotdb.auth;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.security.ITableAuthCheckerImpl;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;

import org.junit.Assert;
import org.junit.Test;

public class DataNodeSeparationOfAdminPowersTest {

  @Test
  public void test() {
    StrictTreeAccessCheckVisitor visitor = new StrictTreeAccessCheckVisitor();
    for (AuthorType value : AuthorType.values()) {
      try {
        TSStatus status =
            visitor.visitAuthor(
                new AuthorStatement(value), new TreeAccessCheckContext(1, "user1", ""));
        if (status.getMessage().contains("Unsupported authorType")) {
          Assert.fail(
              "Unsupported authorType: "
                  + value
                  + " in StrictTreeAccessCheckVisitor.visitAuthor()");
        }
      } catch (Exception ignored) {
      }
    }
    for (AuthorRType value : AuthorRType.values()) {
      StrictAccessControlImpl accessControl =
          new StrictAccessControlImpl(new ITableAuthCheckerImpl(), visitor);
      try {
        accessControl.checkUserCanRunRelationalAuthorStatement(
            "user1", new RelationalAuthorStatement(value), new MPPQueryContext(new QueryId("1")));
      } catch (SemanticException e) {
        if (e.getMessage().contains("Unsupported authorType")) {
          Assert.fail(
              "Unsupported authorType: "
                  + value
                  + " in StrictAccessControlImpl.checkUserCanRunRelationalAuthorStatement()");
        }
      } catch (Exception ignored) {
      }
    }
  }
}
