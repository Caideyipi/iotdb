package org.apache.iotdb.session.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBConnectionNumLimitIT {

  @Test
  public void testTotalConnectionPool() throws SQLException {
    EnvFactory.getEnv().getConfig().getCommonConfig().setDnRpcMaxConcurrentClientNum(2);

    // Init 1C1D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);

    Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);

    Assert.assertThrows(
        SQLException.class,
        () -> {
          EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        });
    adminCon.close();
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSessionLimitPerUser() throws SQLException {
    EnvFactory.getEnv().getConfig().getCommonConfig().setDnRpcMaxConcurrentClientNum(12);

    // Init 1C1D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);

    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      // init users.
      adminStmt.execute("create user user3 'user31234567890'");
      adminStmt.execute("create user user2 'user21234567890'");
      adminStmt.execute("create user user1 'user11234567890'");

      // 1. test maxSessionPerUser
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 0");
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
      Connection user3_1 =
          EnvFactory.getEnv().getConnection("user3", "user31234567890", BaseEnv.TABLE_SQL_DIALECT);
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 1");
      Assert.assertTrue(!user3_1.isClosed()); //
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv()
                .getConnection("user3", "user31234567890", BaseEnv.TABLE_SQL_DIALECT);
          });
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 2");
      Assert.assertTrue(!user3_1.isClosed());
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv()
                .getConnection("user3", "user31234567890", BaseEnv.TABLE_SQL_DIALECT);
          });
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 3");
      // two logins: one login success and one login fails.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv()
                .getConnection("user3", "user31234567890", BaseEnv.TABLE_SQL_DIALECT);
          });
      // two idle connections left.
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 5");
      Connection user3_2 =
          EnvFactory.getEnv().getConnection("user3", "user31234567890", BaseEnv.TABLE_SQL_DIALECT);
      user3_1.close();
      Assert.assertTrue(user3_1.isClosed());
      user3_2.close();
      Assert.assertTrue(user3_2.isClosed());
      // recovery.
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");

      // 2. test minSessionPerUser
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 12");
          });

      adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 10");
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv()
                .getConnection("user2", "user21234567890", BaseEnv.TABLE_SQL_DIALECT);
          });
      adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 8");
      Connection user2Conn =
          EnvFactory.getEnv().getConnection("user2", "user21234567890", BaseEnv.TABLE_SQL_DIALECT);
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user1 SET MIN_SESSION_PER_USER 1");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER 3");
          });
      adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER 2");
      adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 6");
      adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER 3");
      adminStmt.execute("ALTER USer user1 SET MIN_SESSION_PER_USER 1");

      // recovery
      adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user1 SET MIN_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");

      // 3. parameter validation
      // The maximum and minimum number of connections cannot exceed the value of
      // dn_rpc_max_comput_cient_num.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 16");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 16");
          });
      // The minimum number of connections cannot exceed the maximum number of connections.
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 6");
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 5");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 8");
          });
      // Negative numbers less than -1 are not allowed.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -2");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER -3");
          });
      // Decimals are not allowed
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 2.2");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 3.3");
          });
      // Characters are not allowed.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER a");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER b");
          });
      // Fractions are not allowed.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 1/9");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 9/3");
          });
    }
    EnvFactory.getEnv().cleanClusterEnvironment();
  }
}
