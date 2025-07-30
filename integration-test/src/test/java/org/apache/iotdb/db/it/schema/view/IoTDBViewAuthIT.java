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

package org.apache.iotdb.db.it.schema.view;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showLogicalViewColumnHeaders;
import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSeriesPrivilege;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.db.it.utils.TestUtils.revokeUserSeriesPrivilege;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBViewAuthIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createUser("test", "tesT@123456789");
    executeNonQuery("insert into root.sg.d1(time,s1,s2) values(1,1,1),(2,10,10)");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test() {
    testCreate();
    testShow();
    testAlter();
    testRename();
    testDelete();
  }

  private void testCreate() {
    grantUserSeriesPrivilege("test", PrivilegeType.READ_SCHEMA, "root.sg.d1.s1");
    assertNonQueryTestFail(
        "create view root.sg.view_d1.s1 as select (s1+s2)/2 from root.sg.d1",
        "803: No permissions for this operation, please add privilege READ_SCHEMA on [root.sg.d1.s2]",
        "test",
        "tesT@123456789");
    assertNonQueryTestFail(
        "create view root.sg.view_d1.s2 as root.sg.d1.s2",
        "803: No permissions for this operation, please add privilege READ_SCHEMA on [root.sg.d1.s2]",
        "test",
        "tesT@123456789");

    grantUserSeriesPrivilege("test", PrivilegeType.READ_SCHEMA, "root.sg.d1.s2");
    assertNonQueryTestFail(
        "create view root.sg.view_d1.s1 as select (s1+s2)/2 from root.sg.d1",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.sg.view_d1.s1]",
        "test",
        "tesT@123456789");
    assertNonQueryTestFail(
        "create view root.sg.view_d1.s2 as root.sg.d1.s2",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.sg.view_d1.s2]",
        "test",
        "tesT@123456789");

    grantUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg.view_d1.s1");
    executeNonQuery(
        "create view root.sg.view_d1.s1 as select (s1+s2)/2 from root.sg.d1",
        "test",
        "tesT@123456789");
    grantUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg.view_d1.s2");
    executeNonQuery("create view root.sg.view_d1.s2 as root.sg.d1.s2", "test", "tesT@123456789");
  }

  private void testShow() {
    executeNonQuery("create view root.sg.view_d1.s3 as root.sg.d1.s1");
    resultSetEqualTest(
        "show view",
        showLogicalViewColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .toArray(String[]::new),
        new String[] {
          "root.sg.view_d1.s1,root.sg,DOUBLE,null,null,VIEW,(root.sg.d1.s1 + root.sg.d1.s2) / 2,",
          "root.sg.view_d1.s2,root.sg,DOUBLE,null,null,VIEW,root.sg.d1.s2,"
        },
        "test",
        "tesT@123456789");
  }

  private void testAlter() {
    revokeUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg.view_d1.s1");
    revokeUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg.view_d1.s2");
    revokeUserSeriesPrivilege("test", PrivilegeType.READ_SCHEMA, "root.sg.d1.s2");
    assertNonQueryTestFail(
        "alter view root.sg.view_d1.s1 as select (s1+s2)/2 from root.sg.d1",
        "803: No permissions for this operation, please add privilege READ_SCHEMA on [root.sg.d1.s2]",
        "test",
        "tesT@123456789");
    assertNonQueryTestFail(
        "alter view root.sg.view_d1.s2 as root.sg.d1.s2",
        "803: No permissions for this operation, please add privilege READ_SCHEMA on [root.sg.d1.s2]",
        "test",
        "tesT@123456789");

    grantUserSeriesPrivilege("test", PrivilegeType.READ_SCHEMA, "root.sg.d1.s2");
    assertNonQueryTestFail(
        "alter view root.sg.view_d1.s1 as select (s1+s2)/2 from root.sg.d1",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.sg.view_d1.s1]",
        "test",
        "tesT@123456789");
    assertNonQueryTestFail(
        "alter view root.sg.view_d1.s2 as root.sg.d1.s2",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.sg.view_d1.s2]",
        "test",
        "tesT@123456789");

    grantUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg.view_d1.s1");
    executeNonQuery(
        "alter view root.sg.view_d1.s1 as select (s1+s2)/2 from root.sg.d1",
        "test",
        "tesT@123456789");
    grantUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg.view_d1.s2");
    executeNonQuery("alter view root.sg.view_d1.s2 as root.sg.d1.s2", "test", "tesT@123456789");
  }

  private void testRename() {
    // TODO after renaming view is supported
  }

  private void testDelete() {
    revokeUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg.view_d1.s1");
    assertNonQueryTestFail(
        "drop view root.sg.view_d1.s1",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.sg.view_d1.s1]",
        "test",
        "tesT@123456789");
    grantUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg.view_d1.s1");
    executeNonQuery("drop view root.sg.view_d1.s1");
  }
}
