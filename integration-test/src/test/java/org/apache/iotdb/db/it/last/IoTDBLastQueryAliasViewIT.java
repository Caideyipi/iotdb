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

package org.apache.iotdb.db.it.last;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.DATA_TYPE_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESERIES_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.VALUE_STR;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLastQueryAliasViewIT extends IoTDBLastQueryAliasIT {
  protected static final String[] SQLs =
      new String[] {
        "create timeseries root.test.d1.s1 with dataType= int32",
        "create timeseries root.test.d1.s2(alias2) with dataType= int32",
        "create timeseries root.test.d1.s3(alias1) with dataType= int32",
        "insert into root.test.d1(timestamp,s1,s2,s3) values(1,1,2,3)",
        "create view root.test.d1.alias3 as root.test.d1.s1",
        "create aligned timeseries root.test.d2 (s1 int32,s2 (alias2) int32,s3 (alias1) int32)",
        "create view root.test.d2.alias3 as root.test.d2.s1",
        "insert into root.test.d2(timestamp,s1,s2,s3) values(2,2,3,4)",
        "create view root.test.d1.s1adds3 as select s1+s3 from root.test.d1",
        "create view root.test.d2.s1adds3 as select s1+s3 from root.test.d2",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    // with lastCache
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnableLastCache(false);
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      prepareData(SQLs);
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testLastTransform() {
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, TIMESERIES_STR, VALUE_STR, DATA_TYPE_STR};

    String[] retArray =
        new String[] {
          "1,root.test.d1.s1adds3,4.0,DOUBLE,",
        };
    resultSetEqualTest("select last s1adds3 from root.test.d1", expectedHeader, retArray);

    retArray =
        new String[] {
          "1,root.test.d1.s1adds3,4.0,DOUBLE,", "2,root.test.d2.s1adds3,6.0,DOUBLE,",
        };
    resultSetEqualTest("select last s1adds3 from root.test.*", expectedHeader, retArray);

    retArray =
        new String[] {
          "2,root.test.d2.s1adds3,6.0,DOUBLE,",
        };
    resultSetEqualTest("select last s1adds3 from root.test.d2", expectedHeader, retArray);
  }
}
