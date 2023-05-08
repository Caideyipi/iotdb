package com.timecho.iotdb.session;

public class SQLConstants {
  private SQLConstants() {}

  public static final String ALL_DB_SQL =
      "select sum(value) from root.__system.metric.*.*.*.points.*.`type=flush`.*";
  public static final String SPECIFY_DB_SQL_PREFIX =
      "select sum(value) from root.__system.metric.*.*.*.points.";
  public static final String SPECIFY_DB_SQL_SUFFIX = " group by level=6";
  public static final String SPECIFY_DB_SQL_DATABASE = "`database=";
  public static final String SPECIFY_DB_SQL_FLUSH = "`.`type=flush`.*";
  public static final String COMMA = ",";
  public static final String SQL_TIME_GT = " where time >= ";
  public static final String SQL_TIME_LT = " and time >= ";
}
