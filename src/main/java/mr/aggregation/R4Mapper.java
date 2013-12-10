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
  private Map<Integer, Integer> eqJoin = new HashMap<Integer, Integer>();
  private Map<Integer, Integer> lteJoin = new HashMap<Integer, Integer>();
  private Map<Integer, Integer> gteJoin = new HashMap<Integer, Integer>();
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

    String[] eqJoins = context.getConfiguration().get("eqjoin").split(",");
    for (String j : eqJoins) {
      String[] vals = j.split("=");
      eqJoin.put(Integer.parseInt(vals[0]), Integer.parseInt(vals[1]));
    }
    String[] lteJoins = context.getConfiguration().get("ltejoin").split(",");
    for (String j : lteJoins) {
      String[] vals = j.split("=");
      lteJoin.put(Integer.parseInt(vals[0]), Integer.parseInt(vals[1]));
    }
    String[] gteJoins = context.getConfiguration().get("gtejoin").split(",");
    for (String j : gteJoins) {
      String[] vals = j.split("=");
      gteJoin.put(Integer.parseInt(vals[0]), Integer.parseInt(vals[1]));
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
      for (int lpos : eqJoin.keySet()) {
        int rpos = eqJoin.get(lpos);
        res = res && lrow[lpos].equals(rrow[rpos]);
        if (!res) {
          break;
        }
      }
      if (res) {
        for (int lpos : lteJoin.keySet()) {
          int rpos = lteJoin.get(lpos);
          res = res && (coalesce(lrow[lpos], "0").compareTo(rrow[rpos]) <= 0);
          if (!res) {
            break;
          }
        }
      }
      if (res) {
        for (int lpos : gteJoin.keySet()) {
          int rpos = gteJoin.get(lpos);
          res = res && (coalesce(lrow[lpos], "9").compareTo(rrow[rpos]) >= 0);
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

  private String coalesce(String... strings) {
    String curr = null;
    for (String s : strings) {
      if (s != null && !"\\N".equalsIgnoreCase(s)) {
        curr = s;
        break;
      }
    }
    return curr;
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
    String[] rkeys = filter(columns);
    if (rkeys != null) {
      TextMultiple keys = new TextMultiple(columns, lhsKeyPositions);
      TextMultiple vals = new TextMultiple(columns, lhsValPositions);

      keys = new TextMultiple(keys, rkeys);

      context.write(keys, vals);
    }
  }
}
