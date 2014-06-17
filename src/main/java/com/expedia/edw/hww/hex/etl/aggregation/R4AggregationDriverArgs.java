package com.expedia.edw.hww.hex.etl.aggregation;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import com.expedia.edw.hww.common.hadoop.spring.ValidatingBean;
import com.expedia.edw.hww.hex.etl.HexConstants;

@Parameters(separators = "=")
public class R4AggregationDriverArgs implements ValidatingBean {

  @Parameter(names = { HexConstants.REDUCER_COUNT }, description = "Reducer count for the Aggregation step")
  private Integer reducer;

  @Parameter(names = { HexConstants.QUEUE_NAME }, description = "Queue Name for the Aggregation step")
  private String queueName;

  @Parameter(names = { HexConstants.SOURCE_DATABASE_NAME }, description = "Source DB Name for the Aggregation step")
  private String sourceDbName;

  @Parameter(names = { HexConstants.TARGET_DATABASE_NAME }, description = "Target DB Name for the Aggregation step")
  private String targetDbName;

  @Parameter(names = { HexConstants.SOURCE_TABLE_NAME }, description = "Source Table Name for the Aggregation step")
  private String sourceTableName;

  @Parameter(names = { HexConstants.TARGET_TABLE_NAME }, description = "Target Table Name for the Aggregation step")
  private String targetTableName;

  @Parameter(names = { HexConstants.REPORT_TABLE_NAME }, description = "Report Table Name count for the Aggregation step")
  private String reportTableName;

  @Override
  public void validate() {

  }

  public Integer getReducer() {
    return reducer;
  }

  public String getQueueName() {
    return queueName;
  }

  public String getSourceDbName() {
    return sourceDbName;
  }

  public String getTargetDbName() {
    return targetDbName;
  }

  public String getSourceTableName() {
    return sourceTableName;
  }

  public String getTargetTableName() {
    return targetTableName;
  }

  public String getReportTableName() {
    return reportTableName;
  }

}