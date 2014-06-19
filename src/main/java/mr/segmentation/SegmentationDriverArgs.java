package mr.segmentation;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.expedia.edw.hww.hex.etl.CommandLineParameters;

@Parameters(separators = "=")
public class SegmentationDriverArgs {

  @Parameter(names = { CommandLineParameters.REDUCER_COUNT }, description = "Reducer count for the Aggregation step", required = true)
  private Integer reducers;

  @Parameter(names = { CommandLineParameters.QUEUE_NAME }, description = "Queue Name for the Aggregation step", required = true)
  private String queueName;

  @Parameter(names = { CommandLineParameters.SOURCE_DATABASE_NAME }, description = "Source DB Name for the Aggregation step", required = true)
  private String sourceDbName;

  @Parameter(names = { CommandLineParameters.TARGET_DATABASE_NAME }, description = "Target DB Name for the Aggregation step", required = true)
  private String targetDbName;

  @Parameter(names = { CommandLineParameters.SOURCE_TABLE_NAME }, description = "Source Table Name for the Aggregation step", required = true)
  private String sourceTableName;

  @Parameter(names = { CommandLineParameters.TARGET_TABLE_NAME }, description = "Target Table Name for the Aggregation step", required = true)
  private String targetTableName;

  @Parameter(names = { CommandLineParameters.SEGMENTATION_INPUT_FILE_PATH }, description = "Report Table Name count for the Aggregation step", required = true)
  private String segmentationInputFilePath;

  public Integer getSegmentationReducers() {
    return reducers;
  }

  public String getSegmentationQueueName() {
    return queueName;
  }

  public String getSegmentationSourceDbName() {
    return sourceDbName;
  }

  public String getSegmentationTargetDbName() {
    return targetDbName;
  }

  public String getSegmentationSourceTableName() {
    return sourceTableName;
  }

  public String getSegmentationTargetTableName() {
    return targetTableName;
  }

  public String getSegmentationInputFilePath() {
    return segmentationInputFilePath;
  }

}