package com.expedia.edw.hww.hex.etl.aggregation;


import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import com.expedia.edw.hww.hex.etl.CommandLineParameters;

@Parameters(separators = "=")
public class R4AggregationDriverArgs {

  @Parameter(names = { CommandLineParameters.REDUCER_COUNT }, description = "Reducer count for the Aggregation step", required = true)
  private Integer aggregationReducers;

  @Parameter(names = { CommandLineParameters.SOURCE_DATABASE_NAME }, description = "Source DB Name for the Aggregation step", required = true)
  private String aggregationSourceDbName;

  @Parameter(names = { CommandLineParameters.TARGET_DATABASE_NAME }, description = "Target DB Name for the Aggregation step", required = true)
  private String aggregationTargetDbName;

  @Parameter(names = { CommandLineParameters.SOURCE_TABLE_NAME }, description = "Source Table Name for the Aggregation step", required = true)
  private String aggregationSourceTableName;

  @Parameter(names = { CommandLineParameters.TARGET_TABLE_NAME }, description = "Target Table Name for the Aggregation step", required = true)
  private String aggregationTargetTableName;

  @Parameter(names = { CommandLineParameters.REPORT_TABLE_NAME }, description = "Report Table Name count for the Aggregation step", required = true)
  private String aggregationReportTableName;

  public Integer getAggregationReducers() {
    return aggregationReducers;
  }

  public String getAggregationSourceDbName() {
    return aggregationSourceDbName;
  }

  public String getAggregationTargetDbName() {
    return aggregationTargetDbName;
  }

  public String getAggregationSourceTableName() {
    return aggregationSourceTableName;
  }

  public String getAggregationTargetTableName() {
    return aggregationTargetTableName;
  }

  public String getAggregationReportTableName() {
    return aggregationReportTableName;
  }

}