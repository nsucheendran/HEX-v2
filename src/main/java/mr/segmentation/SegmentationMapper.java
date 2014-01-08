package mr.segmentation;

import static mr.Constants.TAB_SEP_PATTERN;
import static mr.utils.Utils.coalesce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import mr.dto.TextMultiple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SegmentationMapper extends Mapper<BytesWritable, Text, TextMultiple, TextMultiple> {

    private int[] lhsValPositions;
    private int[] rhsKeyPositions;
    private TextMultiple keysout;
    private TextMultiple valsout;
    private String[] rkeys;

    private String[][] rtable;
    private FilterCondition eqJoiner, lteJoiner, gteJoiner;
    private SegmentationSpec[] segmentations;

    public SegmentationMapper() {
        super();

    }

    private final void getJoinMap(Map<Integer, Integer> output, String conf) {
        if (!"".equals(conf)) {
            String[] joins = conf.split(",");
            for (String j : joins) {
                String[] vals = j.split("=");
                output.put(Integer.parseInt(vals[0]), Integer.parseInt(vals[1]));
            }
        }
    }

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        ColumnMapping[] colMap = getColMap(conf.get("colMap"));
        segmentations = getSegSpecs(conf.get("segSpecs"), colMap);

        lhsValPositions = getPositions(conf.get("lhsVals"));
        rhsKeyPositions = getPositions(conf.get("rhsKeys"));
        keysout = new TextMultiple(new String[2 + colMap.length + rhsKeyPositions.length]);
        
        valsout = new TextMultiple(new String[lhsValPositions.length]);
        rkeys = new String[rhsKeyPositions.length];
        // lhs-rhs mapped positions of join-participant columns
        // all join conditions are combined into a logical conjunction
        // of match conditions
        Map<Integer, Integer> eqJoin = new HashMap<Integer, Integer>();
        Map<Integer, Integer> lteJoin = new HashMap<Integer, Integer>();
        Map<Integer, Integer> gteJoin = new HashMap<Integer, Integer>();
        getJoinMap(eqJoin, conf.get("eqjoin"));
        getJoinMap(lteJoin, conf.get("ltejoin"));
        getJoinMap(gteJoin, conf.get("gtejoin"));

        // joiners encapsulate the join condition applications represented by the corresponding
        // position maps
        eqJoiner = new FilterCondition(eqJoin) {

            @Override
            protected boolean checkCondition(String lval, String rval) {
                return lval.equals(rval);
            }
        };
        lteJoiner = new FilterCondition(lteJoin) {

            @Override
            protected boolean checkCondition(String lval, String rval) {
                return coalesce(lval, "0").compareTo(rval) <= 0;
            }
        };
        gteJoiner = new FilterCondition(gteJoin) {

            @Override
            protected boolean checkCondition(String lval, String rval) {
                return coalesce(lval, "9").compareTo(rval) >= 0;
            }
        };

        String[] lines = conf.get("data").split("\n");
        rtable = new String[lines.length][];
        int i = 0;
        for (String line : lines) {
            rtable[i++] = line.split("\t");
        }

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
        if (input == null||input.trim().equals("")) {
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

    /*
     * helper class whose derivatives encapsulate join logic as specified by configuration of the context from the job
     */
    private static abstract class FilterCondition {
        private final Map<Integer, Integer> joinMap;

        public FilterCondition(Map<Integer, Integer> joinMap) {
            this.joinMap = joinMap;
        }

        protected abstract boolean checkCondition(String lval, String rval);

        public final boolean satisfiedBy(String[] lrow, String[] rrow) {
            boolean res = true;
            for (int lpos : joinMap.keySet()) {
                int rpos = joinMap.get(lpos);
                res = checkCondition(lrow[lpos], rrow[rpos]);
                if (!res) {
                    break;
                }
            }
            return res;
        }

    }

    /*
     * apply rtable (smaller table) as a filter and emit non-null rtable rows iff the lrow passes the join criteria (technically not
     * inner/right-outer join, but practically an inner join where the join key combinations are assumed to be unique in rtable)
     */
    private final boolean filter(String[] lrow, String[] row) {
        for (String[] rrow : rtable) {
            if (eqJoiner.satisfiedBy(lrow, rrow) && lteJoiner.satisfiedBy(lrow, rrow) && gteJoiner.satisfiedBy(lrow, rrow)) {
                stripe(rrow, rhsKeyPositions, row);
                return true;
            }
        }
        return false;
    }

    private void stripe(String[] rrow, int[] pos, String[] row) {
        int i = 0;
        for (int p : pos) {
            row[i++] = rrow[p];
        }
    }

    @Override
    public void map(BytesWritable ignored, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = TAB_SEP_PATTERN.split(value.toString());

        if (filter(columns, rkeys)) {
            valsout.stripeAppend(columns, lhsValPositions);
            for (SegmentationSpec segpos : segmentations) {
                keysout.stripeFlank(columns, segpos);
                context.write(keysout, valsout);
            }
        }
    }
}
