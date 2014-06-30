package com.expedia.edw.hww.hex.etl.segmentation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.expedia.edw.hww.common.hadoop.spring.BaseContext;

public class R4SegmentationToolTest {

  @Test
  public void testSegmentationDriverArgs() {
    SegmentationTool aggregationTool = new SegmentationTool();
    Class<?> clazz = aggregationTool.getArgumentsClass();

    assertThat(clazz.getName(), is(SegmentationDriverArgs.class.getName()));
  }

  @Test
  public void testSegmentationJob() {
    SegmentationTool aggregationTool = new SegmentationTool();
    Class<?> clazz = aggregationTool.getDriverClass();

    assertThat(clazz.getName(), is(SegmentationJob.class.getName()));
  }

  @Test
  public void testConfigurationClasses() {
    SegmentationTool aggregationTool = new SegmentationTool();
    Class<?>[] classes = aggregationTool.getConfigurationClasses();

    assertThat(classes.length, is(2));
    assertThat(classes[0].getName(), is(BaseContext.class.getName()));
    assertThat(classes[1].getName(), is(SegmentationContext.class.getName()));
  }
}
