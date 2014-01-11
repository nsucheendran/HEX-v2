/*
 * @author achadha
 */

package mr.segmentation;

import static mr.Constants.TAB_SEP_PATTERN;

import java.io.IOException;

import mr.dto.TextMultiple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SegmentationMapper extends Mapper<BytesWritable, Text, TextMultiple, TextMultiple> {

    private int[] lhsValPositions;
    private TextMultiple keysout;
    private TextMultiple valsout;
    private SegmentationSpec[] segmentations;

    public SegmentationMapper() {
        super();
    }

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        ColumnMapping[] colMap = getColMap(conf.get("colMap"));
        segmentations = getSegSpecs(conf.get("segSpecs"), colMap);

        lhsValPositions = getPositions(conf.get("lhsVals"));
        keysout = new TextMultiple(new String[2 + colMap.length]);

        valsout = new TextMultiple(new String[lhsValPositions.length]);
    }

    private SegmentationSpec[] getSegSpecs(String input, ColumnMapping[] colMap) {
        String[] lines = input.split("\n");
        SegmentationSpec[] ret = new SegmentationSpec[lines.length];
        int i = 0;
        for (String line : lines) {
            ret[i++] = new SegmentationSpec(line, colMap);
        }
        return ret;
    }

    private ColumnMapping[] getColMap(String input) {
        String[] lines = input.split("\n");
        ColumnMapping[] ret = new ColumnMapping[lines.length];
        int i = 0;
        for (String line : lines) {
            String[] vals = line.split("\t");
            ret[i++] = new ColumnMapping(Integer.parseInt(vals[0]), vals[1]);
        }
        return ret;
    }

    private int[] getPositions(String input) {
        if (input == null || "".equals(input.trim())) {
            return new int[0];
        }
        String[] posStrs = input.split(",");
        int[] positions = new int[posStrs.length];
        int i = 0;
        for (String posStr : posStrs) {
            positions[i++] = Integer.parseInt(posStr);
        }
        return positions;
    }

    @Override
    public void map(BytesWritable ignored, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = TAB_SEP_PATTERN.split(value.toString());

        valsout.stripeAppend(columns, lhsValPositions);
        for (SegmentationSpec segpos : segmentations) {
            keysout.stripeFlank(columns, segpos);
            context.write(keysout, valsout);
        }
    }
}
