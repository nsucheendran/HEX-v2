package mr.aggregation;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import com.expedia.edw.hww.hex.etl.HexConstants;

@Parameters(separators = "=")
public class R4AggregationDriverArgs {

  @Parameter(names = { HexConstants.REDUCER_COUNT }, description = "Reducer count for the Aggregation step", required = true)
  private Integer aggregationReducers;

  @Parameter(names = { HexConstants.QUEUE_NAME }, description = "Queue Name for the Aggregation step", required = true)
  private String aggregationQueueName;

  @Parameter(names = { HexConstants.SOURCE_DATABASE_NAME }, description = "Source DB Name for the Aggregation step", required = true)
  private String aggregationSourceDbName;

  @Parameter(names = { HexConstants.TARGET_DATABASE_NAME }, description = "Target DB Name for the Aggregation step", required = true)
  private String aggregationTargetDbName;

  @Parameter(names = { HexConstants.SOURCE_TABLE_NAME }, description = "Source Table Name for the Aggregation step", required = true)
  private String aggregationSourceTableName;

  @Parameter(names = { HexConstants.TARGET_TABLE_NAME }, description = "Target Table Name for the Aggregation step", required = true)
  private String aggregationTargetTableName;

  @Parameter(names = { HexConstants.REPORT_TABLE_NAME }, description = "Report Table Name count for the Aggregation step", required = true)
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