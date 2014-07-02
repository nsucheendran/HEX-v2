package com.expedia.edw.hww.hex.etl;

public final class CommandLineParameters {
  /**
   * Constants for command line parameters.
   */
  public static final String REDUCER_COUNT = "--reducers";
  public static final String SOURCE_DATABASE_NAME = "--sourceDbName";
  public static final String TARGET_DATABASE_NAME = "--targetDbName";
  public static final String SOURCE_TABLE_NAME = "--sourceTableName";
  public static final String TARGET_TABLE_NAME = "--targetTableName";
  public static final String REPORT_TABLE_NAME = "--reportTableName";
  public static final String SEGMENTATION_INPUT_FILE_PATH = "--segmentationInputFilePath";

  private CommandLineParameters() {
  }
}
