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
          + "output_length=>%d, "
          + "auto_adapt=>%s"
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
            480,
            "false");
    // Invoke forecast table function for specified models and check the count of results.
    try (ResultSet resultSet = statement.executeQuery(forecastTableFunctionSQL)) {
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      Assert.assertEquals(480, count);
    }

    String historyCovsSQLAutoAdapt = String.format(HISTORY_COVS_TEMPLATE, "s2,s3", 2881, 2881);
    String futureCovsSQLAutoAdapt = String.format(FUTURE_COVS_TEMPLATE, "s3", 2880, 479);

    String forecastTableFunctionSQLAutoAdapt =
        String.format(
            FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
            modelInfo.getModelId(),
            "s0,s1",
            2880,
            2880,
            historyCovsSQLAutoAdapt,
            futureCovsSQLAutoAdapt,
            2880,
            480,
            "true");
    // Invoke forecast table function for specified models and check the count of results.
    try (ResultSet resultSet = statement.executeQuery(forecastTableFunctionSQLAutoAdapt)) {
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
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
            96,
            "true");
    errorTest(
        statement,
        invalidSyntaxSQL,
        "1599: Error occurred while executing forecast:[Future_covs_sql is specified yet history_covs_sql is None.]");

    // Invalid history covariates length
    String invalidHistoryCovsLengthSQL = String.format(HISTORY_COVS_TEMPLATE, "s2,s3", 2879, 2879);
    String invalidHistoryCovariateLengthSQL =
        String.format(
            FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
            modelInfo.getModelId(),
            "s0,s1",
            2880,
            2880,
            invalidHistoryCovsLengthSQL,
            futureCovsSQL,
            2880,
            96,
            "false");
    errorTest(
        statement,
        invalidHistoryCovariateLengthSQL,
        "1599: Error occurred while executing forecast:[Individual `past_covariates` must be 1-d with length equal to the length of `target` (= 2880), found: s2 with shape (2879,) in element at index 0.]");

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
            96,
            "false");
    errorTest(
        statement,
        invalidFutureCovariateLengthSQL,
        "1599: Error occurred while executing forecast:[Individual `future_covariates` must be 1-d with length equal to `output_length` (= 96), found: s3 with shape (95,) in element at index 0.]");

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
            96,
            "false");
    errorTest(
        statement,
        invalidFutureCovariateSetSQL,
        "1599: Error occurred while executing forecast:[Expected keys in `future_covariates` to be a subset of `past_covariates` ['s2', 's3'], but found s1 in element at index 0.]");

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
            96,
            "true");
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
            96,
            "true");
    errorTest(
        statement,
        invalidFutureCovariateEmptySetSQL,
        "1599: Error occurred while executing forecast:[The future covariates are specified but no future data are selected.]");
  }
}
