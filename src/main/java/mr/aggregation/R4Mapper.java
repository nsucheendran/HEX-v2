package mr.aggregation;

import static mr.Constants.TAB_SEP_PATTERN;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import mr.dto.TextMultiple;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class R4Mapper extends Mapper<BytesWritable, Text, TextMultiple, TextMultiple> {

    private int[] lhsKeyPositions;
    private int[] lhsValPositions;
    private int[] rhsKeyPositions;
    // private int[] rhsValPositions;
    private Map<Integer, Integer> join = new HashMap<Integer, Integer>();
    private String[][] rtable;

    public R4Mapper() {
        super();
    }

    @Override
    public void setup(Context context) {
        lhsKeyPositions = getPositions(context, "lhsKeys");
        lhsValPositions = getPositions(context, "lhsVals");

        rhsKeyPositions = getPositions(context, "rhsKeys");
        // rhsValPositions = getPositions(context, "rhsVals");

        String[] joins = context.getConfiguration().get("join").split(",");
        for (String j : joins) {
            String[] vals = j.split("=");
            join.put(Integer.parseInt(vals[0]), Integer.parseInt(vals[1]));
        }
        
        String[] lines = context.getConfiguration().get("data").split("\n");
        rtable = new String[lines.length][];
        int i = 0;
        for (String line : lines) {
            rtable[i++] = line.split("\t");
        }

    }

    private int[] getPositions(Context context, String attr) {
        String[] posStrs = context.getConfiguration().get(attr).split(",");
        int[] positions = new int[posStrs.length];
        int i = 0;
        for (String posStr : posStrs) {
            positions[i++] = Integer.parseInt(posStr);
        }
        return positions;
    }


    private final String[] filter(String[] lrow) {

        for (String[] rrow : rtable) {
            boolean res = true;
            for (int lpos : join.keySet()) {
                int rpos = join.get(lpos);
                res = res && lrow[lpos].equals(rrow[rpos]);
                if (!res)
                    break;
            }
            if (res) {
                return stripe(rrow, rhsKeyPositions);
            }
        }
        return null;
    }

    private String[] stripe(String[] rrow, int[] pos) {
        String[] row = new String[pos.length];
        int i = 0;
        for (int p : pos) {
            row[i++] = rrow[p];
        }
        return row;
    }

    @Override
    public void map(BytesWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = TAB_SEP_PATTERN.split(value.toString());
        String[] rkeys;
        if ((rkeys = filter(columns)) != null) {
            TextMultiple keys = new TextMultiple(columns, lhsKeyPositions);
            TextMultiple vals = new TextMultiple(columns, lhsValPositions);
            
            keys = new TextMultiple(keys, rkeys);

            context.write(keys, vals);
        }
    }
}
