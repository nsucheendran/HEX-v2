package mr.aggregation;

import com.expedia.edw.hww.common.hadoop.spring.AbstractSpringTool;
import com.expedia.edw.hww.common.hadoop.spring.BaseContext;
import com.expedia.edw.hww.common.hadoop.spring.DriverEntryPoint;

public class R4AggregationTool extends AbstractSpringTool {

  @Override
  protected Class<?> getArgumentsClass() {
    return R4AggregationDriverArgs.class;
  }

  @Override
  protected Class<?>[] getConfigurationClasses() {
    return new Class<?>[] { BaseContext.class, R4AggregationContext.class };
  }

  @Override
  protected Class<? extends DriverEntryPoint> getDriverClass() {
    return R4AggregationJob.class;
  }

  public static void main(String[] args) {
    int status = new R4AggregationTool().start(args);
    if (status != 0) {
      System.exit(status);
    }
  }
}
