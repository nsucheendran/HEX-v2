package com.expedia.edw.hww.hex.etl.aggregation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.expedia.edw.hww.common.hadoop.spring.BaseContext;

public class R4AggregationToolTest {

  @Test
  public void testAggregationDriverArgs() {
    R4AggregationTool aggregationTool = new R4AggregationTool();
    Class<?> clazz = aggregationTool.getArgumentsClass();

    assertThat(clazz.getName(), is(R4AggregationDriverArgs.class.getName()));
  }

  @Test
  public void testAggregationJob() {
    R4AggregationTool aggregationTool = new R4AggregationTool();
    Class<?> clazz = aggregationTool.getDriverClass();

    assertThat(clazz.getName(), is(R4AggregationJob.class.getName()));
  }

  @Test
  public void testConfigurationClasses() {
    R4AggregationTool aggregationTool = new R4AggregationTool();
    Class<?>[] classes = aggregationTool.getConfigurationClasses();

    assertThat(classes.length, is(2));
    assertThat(classes[0].getName(), is(BaseContext.class.getName()));
    assertThat(classes[1].getName(), is(R4AggregationContext.class.getName()));
  }
}
