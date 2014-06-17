package com.expedia.edw.hww.hex.etl.aggregation;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import com.expedia.edw.hww.common.hadoop.metrics.StatsWriterFactoryBean;

@Configuration
@ComponentScan("com.expedia.edw.hww.hex.etl.aggregation")
public class R4AggregationContext {

  @Bean
  public static PropertySourcesPlaceholderConfigurer placeHolderConfigurer() {
    PropertySourcesPlaceholderConfigurer placeholderConfigurer = new PropertySourcesPlaceholderConfigurer();
    placeholderConfigurer.setNullValue("null");
    return placeholderConfigurer;
  }

  @Bean
  public StatsWriterFactoryBean statsWriter() {
    return new StatsWriterFactoryBean();
  }
}