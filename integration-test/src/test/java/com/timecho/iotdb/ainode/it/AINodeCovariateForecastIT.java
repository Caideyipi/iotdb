package com.timecho.iotdb.ainode.it;

import org.apache.iotdb.ainode.utils.AINodeTestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.AIClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.BUILTIN_MODEL_MAP;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.errorTest;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.prepareDataInTable;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeCovariateForecastIT {
  private static final String FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE =
      "SELECT * FROM FORECAST("
          + "model_id=>'%s', "
          + "targets=>(SELECT time, %s FROM db.AI WHERE time<%d ORDER BY time DESC LIMIT %d) ORDER BY time, "
          + "history_covs=>'%s',"
          + "future_covs=>'%s',"
          + "output_start_time=>%d, "
          + "output_length=>%d"
          + ")";
  private static final String HISTORY_COVS_TEMPLATE =
      "(SELECT time, %s FROM db.AI WHERE time < %d ORDER BY time DESC LIMIT %d) ORDER BY time";

  private static final String FUTURE_COVS_TEMPLATE =
      "SELECT time, %s FROM db.AI WHERE time >= %d ORDER BY time LIMIT %d";

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1A cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    prepareDataInTable();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void forecastTableFunctionWithCovariateTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      forecastTableFunctionWithCovariateTest(statement, BUILTIN_MODEL_MAP.get("chronos2"));
    }
  }

  public static void forecastTableFunctionWithCovariateTest(
      Statement statement, AINodeTestUtils.FakeModelInfo modelInfo) throws SQLException {

    String historyCovsSQL = String.format(HISTORY_COVS_TEMPLATE, "s2,s3", 2880, 2880);
    String futureCovsSQL = String.format(FUTURE_COVS_TEMPLATE, "s3", 2880, 480);

    String forecastTableFunctionSQL =
        String.format(
            FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
            modelInfo.getModelId(),
            "s0,s1",
            2880,
            2880,
            historyCovsSQL,
            futureCovsSQL,
            2880,
            480);
    // Invoke forecast table function for specified models, there should exist result.
    try (ResultSet resultSet = statement.executeQuery(forecastTableFunctionSQL)) {
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      // Ensure the forecast sentence return results
      Assert.assertEquals(480, count);
    }
  }

  @Test
  public void forecastTableFunctionWithCovariateErrorTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      forecastTableFunctionWithCovariateErrorTest(statement, BUILTIN_MODEL_MAP.get("chronos2"));
    }
  }

  public static void forecastTableFunctionWithCovariateErrorTest(
      Statement statement, AINodeTestUtils.FakeModelInfo modelInfo) {

    String historyCovsSQL = String.format(HISTORY_COVS_TEMPLATE, "s2,s3", 2880, 2880);
    String futureCovsSQL = String.format(FUTURE_COVS_TEMPLATE, "s3", 2880, 96);

    // Future_covs_sql is specified yet history_covs_sql is None
    String invalidSyntaxSQL =
        String.format(
            FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
            modelInfo.getModelId(),
            "s0,s1",
            2880,
            2880,
            "",
            futureCovsSQL,
            2880,
            96);
    errorTest(
        statement,
        invalidSyntaxSQL,
        "1599: Error occurred while executing forecast:[Future_covs_sql is specified yet history_covs_sql is None.]");

    // Invalid future covariates length
    String invalidFutureCovsLengthSQL = String.format(FUTURE_COVS_TEMPLATE, "s3", 2880, 95);
    String invalidFutureCovariateLengthSQL =
        String.format(
            FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
            modelInfo.getModelId(),
            "s0,s1",
            2880,
            2880,
            historyCovsSQL,
            invalidFutureCovsLengthSQL,
            2880,
            96);
    errorTest(
        statement,
        invalidFutureCovariateLengthSQL,
        "1599: Error occurred while executing forecast:[Each covariate in 'future_covariates' must have shape (96,), but got shape torch.Size([95]) for key 's3' at index 0.]");

    // Invalid future covariates set
    String invalidFutureCovsSetSQL = String.format(FUTURE_COVS_TEMPLATE, "s1", 2880, 96);
    String invalidFutureCovariateSetSQL =
        String.format(
            FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
            modelInfo.getModelId(),
            "s0,s1",
            2880,
            2880,
            historyCovsSQL,
            invalidFutureCovsSetSQL,
            2880,
            96);
    errorTest(
        statement,
        invalidFutureCovariateSetSQL,
        "1599: Error occurred while executing forecast:[Key 's1' in 'future_covariates' is not in 'past_covariates' at index 0.]");

    // Empty set - history covariates
    String invalidHistoryCovsEmptySetSQL = String.format(HISTORY_COVS_TEMPLATE, "s2,s3", 0, 96);
    String invalidHistoryCovariateEmptySetSQL =
        String.format(
            FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
            modelInfo.getModelId(),
            "s0,s1",
            2880,
            2880,
            invalidHistoryCovsEmptySetSQL,
            futureCovsSQL,
            2880,
            96);
    errorTest(
        statement,
        invalidHistoryCovariateEmptySetSQL,
        "1599: Error occurred while executing forecast:[The history covariates are specified but no history data are selected.]");

    // Empty set - future covariates
    String invalidFutureCovsEmptySetSQL = String.format(FUTURE_COVS_TEMPLATE, "s3", 5760, 96);
    String invalidFutureCovariateEmptySetSQL =
        String.format(
            FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
            modelInfo.getModelId(),
            "s0,s1",
            2880,
            2880,
            historyCovsSQL,
            invalidFutureCovsEmptySetSQL,
            2880,
            96);
    errorTest(
        statement,
        invalidFutureCovariateEmptySetSQL,
        "1599: Error occurred while executing forecast:[The future covariates are specified but no future data are selected.]");
  }
}
