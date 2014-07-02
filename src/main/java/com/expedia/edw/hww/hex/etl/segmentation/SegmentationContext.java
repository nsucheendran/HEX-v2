package com.expedia.edw.hww.hex.etl.segmentation;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.expedia.edw.hww.common.hadoop.metrics.StatsWriterFactoryBean;

@Configuration
@ComponentScan("com.expedia.edw.hww.hex.etl.segmentation")
public class SegmentationContext {

  @Bean
  public StatsWriterFactoryBean statsWriter() {
    return new StatsWriterFactoryBean();
  }
}