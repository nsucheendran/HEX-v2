package mr.aggregation;

import static mr.Constants.TAB_SEP_PATTERN;
import static mr.utils.Utils.coalesce;

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
  private final Map<Integer, Integer> eqJoin = new HashMap<Integer, Integer>();
  private final Map<Integer, Integer> lteJoin = new HashMap<Integer, Integer>();
  private final Map<Integer, Integer> gteJoin = new HashMap<Integer, Integer>();
  private String[][] rtable;

  public R4Mapper() {
    super();
  }

  private final void parsePosMap(Map<Integer, Integer> output, String conf) {
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
    lhsKeyPositions = getPositions(context, "lhsKeys");
    lhsValPositions = getPositions(context, "lhsVals");

    rhsKeyPositions = getPositions(context, "rhsKeys");

    parsePosMap(eqJoin, context.getConfiguration().get("eqjoin"));
    parsePosMap(lteJoin, context.getConfiguration().get("ltejoin"));
    parsePosMap(gteJoin, context.getConfiguration().get("gtejoin"));

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
      for (int lpos : eqJoin.keySet()) {
        int rpos = eqJoin.get(lpos);
        res = lrow[lpos].equals(rrow[rpos]);
        if (!res) {
          break;
        }
      }
      if (res) {
        for (int lpos : lteJoin.keySet()) {
          int rpos = lteJoin.get(lpos);
          res = coalesce(lrow[lpos], "0").compareTo(rrow[rpos]) <= 0;
          if (!res) {
            break;
          }
        }
      }
      if (res) {
        for (int lpos : gteJoin.keySet()) {
          int rpos = gteJoin.get(lpos);
          res = coalesce(lrow[lpos], "9").compareTo(rrow[rpos]) >= 0;
          if (!res) {
            break;
          }
        }
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
  public void map(BytesWritable ignored, Text value, Context context) throws IOException, InterruptedException {
    String[] columns = TAB_SEP_PATTERN.split(value.toString());
    String[] rkeys = filter(columns);
    if (rkeys != null) {
      TextMultiple keys = new TextMultiple(columns, lhsKeyPositions);
      TextMultiple vals = new TextMultiple(columns, lhsValPositions);

      keys = new TextMultiple(keys, rkeys);

      context.write(keys, vals);
    }
  }
}
