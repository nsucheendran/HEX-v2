package com.expedia.edw.hww.hex.etl.segmentation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import com.expedia.edw.hww.hex.etl.CommandLineParameters;
import com.expedia.edw.hww.hex.etl.segmentation.SegmentationDriverArgs;

public class SegmentationDriverArgsTest {

  private final String queueName = "edwdev";
  private final String reducerCount = "250";
  private final String sourceDbName = "sourceDB";
  private final String targetDbName = "targetDB";
  private final String sourceTableName = "sourceTable";
  private final String targetTableName = "targetTable";
  private final String segmentationFileInputPath = "segmentationFileInputPath";

  @Test
  public void validParamTest() {
    SegmentationDriverArgs driverArgs = new SegmentationDriverArgs();

    new JCommander(driverArgs, new String[] { CommandLineParameters.QUEUE_NAME, queueName,
        CommandLineParameters.REDUCER_COUNT, reducerCount, CommandLineParameters.SOURCE_DATABASE_NAME, sourceDbName,
        CommandLineParameters.TARGET_DATABASE_NAME, targetDbName, CommandLineParameters.SOURCE_TABLE_NAME,
        sourceTableName, CommandLineParameters.TARGET_TABLE_NAME, targetTableName,
        CommandLineParameters.SEGMENTATION_INPUT_FILE_PATH, segmentationFileInputPath });

    assertThat(driverArgs.getSegmentationQueueName(), is(queueName));
    assertThat(driverArgs.getSegmentationReducers(), is(Integer.valueOf(reducerCount)));
    assertThat(driverArgs.getSegmentationSourceDbName(), is(sourceDbName));
    assertThat(driverArgs.getSegmentationTargetDbName(), is(targetDbName));
    assertThat(driverArgs.getSegmentationSourceTableName(), is(sourceTableName));
    assertThat(driverArgs.getSegmentationTargetTableName(), is(targetTableName));
    assertThat(driverArgs.getSegmentationInputFilePath(), is(segmentationFileInputPath));
  }

  @Test(expected = ParameterException.class)
  public void noArgsTest() {
    SegmentationDriverArgs driverArgs = new SegmentationDriverArgs();

    new JCommander(driverArgs, new String[0]);
  }

  @Test(expected = ParameterException.class)
  public void missingArgsTest() {
    SegmentationDriverArgs driverArgs = new SegmentationDriverArgs();

    new JCommander(driverArgs, new String[] { CommandLineParameters.QUEUE_NAME, queueName,
        CommandLineParameters.REDUCER_COUNT, reducerCount, CommandLineParameters.SOURCE_DATABASE_NAME, sourceDbName,
        CommandLineParameters.TARGET_DATABASE_NAME, targetDbName, CommandLineParameters.SOURCE_TABLE_NAME,
        sourceTableName, CommandLineParameters.TARGET_TABLE_NAME, targetTableName });
  }
}