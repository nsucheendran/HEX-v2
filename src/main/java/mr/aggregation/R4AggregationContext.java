package mr.aggregation;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.expedia.edw.hww.common.hadoop.metrics.StatsWriterFactoryBean;

@Configuration
@ComponentScan("mr.aggregation")
public class R4AggregationContext {
  @Bean
  public StatsWriterFactoryBean statsWriter() {
    return new StatsWriterFactoryBean();
  }
}