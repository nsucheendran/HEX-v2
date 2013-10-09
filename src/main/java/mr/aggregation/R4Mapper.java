package mr.aggregation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import mr.dto.TextMultiple;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import static mr.Constants.*;

public class R4Mapper extends MapReduceBase implements Mapper<Text, Text, TextMultiple, TextMultiple> {
	
	/*public void configure(JobConf conf) {
		BufferedReader br = new BufferedReader(new FileReader(conf.get(SEGMENTATION_DATA_PATH_KEY)));
		String line;

		while ((line = br.readLine()) != null) {
			String fields[] = line.split(OUT_DELIIM);
			endTrans.put(fields[FIELD_DM_RPT_INPUT_KEY],
					fields[FIELD_DM_RPT_END_TRANSACTION_DATE]);
		}
		br.close();
			
		}
		catch(Exception e){
			initErrorString = e.toString();
			initError = true;
		}
    }*/
	
    public void map(Text key, Text value, OutputCollector<TextMultiple, TextMultiple> output, Reporter reporter)
            throws IOException {
        String line = value.toString();
        //output.collect(null, line);
        String[] columns = TAB_SEP_PATTERN.split( line );
        output.collect( new TextMultiple(Integer.toString(CID_SEGMENTATION_KEY), 
        		columns[EXPERIMENT_CODE_POSITION].equals(HIVE_NULL_VALUE)?"":columns[EXPERIMENT_CODE_POSITION], 
        		columns[VARIANT_CODE_POSITION].equals(HIVE_NULL_VALUE)?"":columns[VARIANT_CODE_POSITION], 
        		columns[VERSION_NUMBER_POSITION].equals(HIVE_NULL_VALUE)?"":columns[VERSION_NUMBER_POSITION], 
        		columns[CID_POSITION].equals(HIVE_NULL_VALUE)?"":columns[CID_POSITION],
                columns[LOCAL_DATE_POSITION].equals(HIVE_NULL_VALUE)?"":columns[LOCAL_DATE_POSITION]), new TextMultiple(columns[GUID_POSITION], 
                		columns[TRANS_DATE_POSITION], columns[NUM_TRANS_POSITION], 
                		columns[BKG_GBV_POSITION],
                        columns[BKG_RN_POSITION],
                        columns[GROSS_PROFIT_POSITION]));
    }
}