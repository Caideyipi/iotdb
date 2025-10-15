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

package com.timecho.iotdb.persistence.auth;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.persistence.auth.AuthorInfo;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;

import org.junit.Assert;
import org.junit.Test;

public class SeparationOfAdminPowersTest {

  @Test
  public void test() {
    StrictAuthorPlanExecutor executor = new StrictAuthorPlanExecutor(null, null);
    for (AuthorType authorType : AuthorType.values()) {
      if (authorType == AuthorType.LIST_USER
          || authorType == AuthorType.LIST_ROLE
          || authorType == AuthorType.LIST_USER_PRIVILEGE
          || authorType == AuthorType.LIST_ROLE_PRIVILEGE
          || authorType == AuthorType.ACCOUNT_UNLOCK) {
        continue;
      }
      ConfigPhysicalPlanType planType =
          AuthorInfo.getConfigPhysicalPlanTypeFromAuthorType(authorType.ordinal());
      try {
        TSStatus status = executor.executeAuthorNonQuery(new AuthorTreePlan(planType));
        if (status.getMessage().contains("Unsupported author type")) {
          Assert.fail(
              "Unsupported author type "
                  + authorType.name()
                  + " in StrictAuthorPlanExecutor.executeAuthorNonQuery()");
        }
      } catch (Exception ignored) {
      }
    }
    for (AuthorRType authorType : AuthorRType.values()) {
      if (authorType == AuthorRType.LIST_USER
          || authorType == AuthorRType.LIST_ROLE
          || authorType == AuthorRType.LIST_USER_PRIV
          || authorType == AuthorRType.LIST_ROLE_PRIV
          || authorType == AuthorRType.ACCOUNT_UNLOCK) {
        continue;
      }
      ConfigPhysicalPlanType planType =
          AuthorInfo.getConfigPhysicalPlanTypeFromAuthorRType(authorType.ordinal());
      try {
        TSStatus status =
            executor.executeRelationalAuthorNonQuery(new AuthorRelationalPlan(planType));
        if (status.getMessage().contains("Unsupported author type")) {
          Assert.fail(
              "Unsupported author type "
                  + authorType.name()
                  + " in StrictAuthorPlanExecutor.executeRelationalAuthorNonQuery()");
        }
      } catch (Exception ignored) {
      }
    }
  }
}
