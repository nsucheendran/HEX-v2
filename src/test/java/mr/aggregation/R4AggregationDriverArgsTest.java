package mr.aggregation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import com.expedia.edw.hww.hex.etl.CommandLineParameters;

public class R4AggregationDriverArgsTest {

  private final String queueName = "edwdev";
  private final String reducerCount = "250";
  private final String sourceDbName = "sourceDB";
  private final String targetDbName = "targetDB";
  private final String sourceTableName = "sourceTable";
  private final String targetTableName = "targetTable";
  private final String reportTableName = "reportTable";

  @Test
  public void validParamTest() {
    R4AggregationDriverArgs driverArgs = new R4AggregationDriverArgs();

    new JCommander(driverArgs, new String[] { CommandLineParameters.QUEUE_NAME, queueName, CommandLineParameters.REDUCER_COUNT, reducerCount,
        CommandLineParameters.SOURCE_DATABASE_NAME, sourceDbName, CommandLineParameters.TARGET_DATABASE_NAME, targetDbName,
        CommandLineParameters.SOURCE_TABLE_NAME, sourceTableName, CommandLineParameters.TARGET_TABLE_NAME, targetTableName,
        CommandLineParameters.REPORT_TABLE_NAME, reportTableName });

    assertThat(driverArgs.getAggregationQueueName(), is(queueName));
    assertThat(driverArgs.getAggregationReducers(), is(Integer.valueOf(reducerCount)));
    assertThat(driverArgs.getAggregationSourceDbName(), is(sourceDbName));
    assertThat(driverArgs.getAggregationTargetDbName(), is(targetDbName));
    assertThat(driverArgs.getAggregationSourceTableName(), is(sourceTableName));
    assertThat(driverArgs.getAggregationTargetTableName(), is(targetTableName));
    assertThat(driverArgs.getAggregationReportTableName(), is(reportTableName));
  }

  @Test(expected = ParameterException.class)
  public void noArgsTest() {
    R4AggregationDriverArgs driverArgs = new R4AggregationDriverArgs();

    new JCommander(driverArgs, new String[0]);
  }

  @Test(expected = ParameterException.class)
  public void missingArgsTest() {
    R4AggregationDriverArgs driverArgs = new R4AggregationDriverArgs();

    new JCommander(driverArgs, new String[] { CommandLineParameters.QUEUE_NAME, queueName, CommandLineParameters.REDUCER_COUNT, reducerCount,
        CommandLineParameters.SOURCE_DATABASE_NAME, sourceDbName, CommandLineParameters.TARGET_DATABASE_NAME, targetDbName,
        CommandLineParameters.SOURCE_TABLE_NAME, sourceTableName, CommandLineParameters.TARGET_TABLE_NAME, targetTableName });
  }
}
