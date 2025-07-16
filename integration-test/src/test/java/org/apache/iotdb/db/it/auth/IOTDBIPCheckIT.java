package org.apache.iotdb.db.it.auth;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.IoTDBSetConfigurationIT.checkConfigFileContains;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IOTDBIPCheckIT {
  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSetWhiteListWithExactMatch() {
    try {
      Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
      Statement adminStmt = adminCon.createStatement();
      adminStmt.execute("set configuration enable_white_list='true' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_white_list=true", "enable_white_list=true")));
      adminStmt.execute("set configuration enable_black_list='false' ");

      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_black_list=false", "enable_black_list=false")));
      adminStmt.execute("set configuration white_ip_list='127.0.0.1' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "white_ip_list=127.0.0.1", "white_ip_list=127.0.0.1")));
      adminCon.close();
      Assert.assertTrue(adminCon.isClosed());
      EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSetWhiteListWithFuzzyMatch() {
    try {
      Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
      Statement adminStmt = adminCon.createStatement();
      adminStmt.execute("set configuration enable_white_list='true' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_white_list=true", "enable_white_list=true")));
      adminStmt.execute("set configuration enable_black_list='false' ");

      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_black_list=false", "enable_black_list=false")));
      adminStmt.execute("set configuration white_ip_list='127.0.*.*' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "white_ip_list=127.0.*.*", "white_ip_list=127.0.*.*")));
      adminCon.close();
      Assert.assertTrue(adminCon.isClosed());
      EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSetWhiteListWithNull() {
    try {
      Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
      Statement adminStmt = adminCon.createStatement();
      adminStmt.execute("set configuration enable_white_list='true' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_white_list=true", "enable_white_list=true")));
      adminStmt.execute("set configuration enable_black_list='false' ");

      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_black_list=false", "enable_black_list=false")));
      adminStmt.execute("set configuration white_ip_list='' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(nodeWrapper, "white_ip_list=", "white_ip_list=")));
      adminCon.close();
      Assert.assertTrue(adminCon.isClosed());
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          });

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSetBlackListWithExactMatch() {
    try {
      Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
      Statement adminStmt = adminCon.createStatement();
      adminStmt.execute("set configuration enable_black_list='true' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_black_list=true", "enable_black_list=true")));
      adminStmt.execute("set configuration enable_white_list='false' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_white_list=false", "enable_white_list=false")));
      adminStmt.execute("set configuration black_ip_list='127.0.0.1' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "black_ip_list=127.0.0.1", "black_ip_list=127.0.0.1")));
      adminCon.close();
      Assert.assertTrue(adminCon.isClosed());
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          });

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSetBlackListWithFuzzyMatch() {
    try {
      Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
      Statement adminStmt = adminCon.createStatement();
      adminStmt.execute("set configuration enable_black_list='true' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_black_list=true", "enable_black_list=true")));
      adminStmt.execute("set configuration enable_white_list='false' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_white_list=false", "enable_white_list=false")));
      adminStmt.execute("set configuration black_ip_list='127.0.*.*' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "black_ip_list=127.0.*.*", "black_ip_list=127.0.*.*")));
      adminCon.close();
      Assert.assertTrue(adminCon.isClosed());
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          });

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSetBlackListWithNull() {
    try {
      Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
      Statement adminStmt = adminCon.createStatement();
      adminStmt.execute("set configuration enable_black_list='true' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_black_list=true", "enable_black_list=true")));
      adminStmt.execute("set configuration enable_white_list='false' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_white_list=false", "enable_white_list=false")));
      adminStmt.execute("set configuration black_ip_list='' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(nodeWrapper, "black_ip_list=", "black_ip_list=")));
      adminCon.close();
      Assert.assertTrue(adminCon.isClosed());
      EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSetIPBothInBlackAndWhiteList() {
    try {
      Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
      Statement adminStmt = adminCon.createStatement();
      adminStmt.execute("set configuration enable_black_list='true' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_black_list=true", "enable_black_list=true")));
      adminStmt.execute("set configuration enable_white_list='true' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_white_list=true", "enable_white_list=true")));
      adminStmt.execute("set configuration black_ip_list='127.0.0.1' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "black_ip_list=127.0.0.1", "black_ip_list=127.0.0.1")));
      adminStmt.execute("set configuration white_ip_list='127.0.0.1' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "white_ip_list=127.0.0.1", "white_ip_list=127.0.0.1")));
      adminCon.close();
      Assert.assertTrue(adminCon.isClosed());
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          });
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
