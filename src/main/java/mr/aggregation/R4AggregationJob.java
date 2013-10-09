package mr.aggregation;

import java.io.IOException;

import mr.dto.TextMultiple;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;

import static mr.Constants.*;

public class R4AggregationJob {
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf( R4AggregationJob.class );
        conf.setJobName( "R4_Aggregation" );
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        /*conf.set(REPORTING_DATA_PATH_KEY, args[4]);
        conf.set(SEGMENTATION_DATA_PATH_KEY, args[5]);*/
        FileOutputFormat.setCompressOutput(conf, true);
        FileOutputFormat.setOutputCompressorClass(conf,org.apache.hadoop.io.compress.SnappyCodec.class);
        conf.setMapperClass(R4Mapper.class);
        conf.setReducerClass(R4Reducer.class);
        conf.setInputFormat(SequenceFileAsTextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setCompressMapOutput(true);
        conf.setMapOutputCompressorClass(org.apache.hadoop.io.compress.SnappyCodec.class);
        /* 
         * NumReducers=2, for testing on small dataset
         */
        conf.setNumReduceTasks(2);
        conf.setOutputKeyClass(TextMultiple.class);
        conf.setOutputValueClass(TextMultiple.class);
        conf.setMapOutputKeyClass(TextMultiple.class);
        conf.setMapOutputValueClass(TextMultiple.class);
        //conf.setQueueName(args[3]);
        //conf.setQueueName("edwdev");
        JobClient.runJob( conf );       
        
	}
}