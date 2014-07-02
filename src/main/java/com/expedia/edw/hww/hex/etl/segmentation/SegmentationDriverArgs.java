package com.expedia.edw.hww.hex.etl.segmentation;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import com.expedia.edw.hww.hex.etl.CommandLineParameters;

@Parameters(separators = "=")
public class SegmentationDriverArgs {

  @Parameter(names = { CommandLineParameters.REDUCER_COUNT }, description = "Reducer count for the Segmentation step", required = true)
  private Integer reducers;

  @Parameter(names = { CommandLineParameters.SOURCE_DATABASE_NAME }, description = "Source DB Name for the Segmentation step", required = true)
  private String sourceDbName;

  @Parameter(names = { CommandLineParameters.TARGET_DATABASE_NAME }, description = "Target DB Name for the Segmentation step", required = true)
  private String targetDbName;

  @Parameter(names = { CommandLineParameters.SOURCE_TABLE_NAME }, description = "Source Table Name for the Segmentation step", required = true)
  private String sourceTableName;

  @Parameter(names = { CommandLineParameters.TARGET_TABLE_NAME }, description = "Target Table Name for the Segmentation step", required = true)
  private String targetTableName;

  @Parameter(names = { CommandLineParameters.SEGMENTATION_INPUT_FILE_PATH }, description = "Input file path for the Segmentation step", required = true)
  private String segmentationInputFilePath;

  public Integer getSegmentationReducers() {
    return reducers;
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