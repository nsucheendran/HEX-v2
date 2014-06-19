package mr.segmentation;

import com.expedia.edw.hww.common.hadoop.spring.AbstractSpringTool;
import com.expedia.edw.hww.common.hadoop.spring.BaseContext;
import com.expedia.edw.hww.common.hadoop.spring.DriverEntryPoint;

public class SegmentationTool extends AbstractSpringTool {

  @Override
  protected Class<?> getArgumentsClass() {
    return SegmentationDriverArgs.class;
  }

  @Override
  protected Class<?>[] getConfigurationClasses() {
    return new Class<?>[] { BaseContext.class, SegmentationContext.class };
  }

  @Override
  protected Class<? extends DriverEntryPoint> getDriverClass() {
    return SegmentationJob.class;
  }

  public static void main(String[] args) {
    int status = new SegmentationTool().start(args);
    if (status != 0) {
      System.exit(status);
    }
  }
}
