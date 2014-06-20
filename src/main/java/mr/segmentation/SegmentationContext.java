package mr.segmentation;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.expedia.edw.hww.common.hadoop.metrics.StatsWriterFactoryBean;

@Configuration
@ComponentScan("mr.segmentation")
public class SegmentationContext {

  @Bean
  public StatsWriterFactoryBean statsWriter() {
    return new StatsWriterFactoryBean();
  }
}