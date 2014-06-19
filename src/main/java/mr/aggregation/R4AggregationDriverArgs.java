package mr.aggregation;

import mr.Constants;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=")
public class R4AggregationDriverArgs {

  @Parameter(names = { Constants.REDUCER_COUNT }, description = "Reducer count for the Aggregation step", required = true)
  private Integer aggregationReducers;

  @Parameter(names = { Constants.QUEUE_NAME }, description = "Queue Name for the Aggregation step", required = true)
  private String aggregationQueueName;

  @Parameter(names = { Constants.SOURCE_DATABASE_NAME }, description = "Source DB Name for the Aggregation step", required = true)
  private String aggregationSourceDbName;

  @Parameter(names = { Constants.TARGET_DATABASE_NAME }, description = "Target DB Name for the Aggregation step", required = true)
  private String aggregationTargetDbName;

  @Parameter(names = { Constants.SOURCE_TABLE_NAME }, description = "Source Table Name for the Aggregation step", required = true)
  private String aggregationSourceTableName;

  @Parameter(names = { Constants.TARGET_TABLE_NAME }, description = "Target Table Name for the Aggregation step", required = true)
  private String aggregationTargetTableName;

  @Parameter(names = { Constants.REPORT_TABLE_NAME }, description = "Report Table Name count for the Aggregation step", required = true)
  private String aggregationReportTableName;

  public Integer getAggregationReducers() {
    return aggregationReducers;
  }

  public String getAggregationQueueName() {
    return aggregationQueueName;
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