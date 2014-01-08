/*
 * @author achadha
 */

package mr.segmentation;

import java.util.HashMap;
import java.util.Map;

import mr.utils.Utils;

import org.apache.hadoop.io.Text;

public final class SegmentationSpec {
    private String[] specData;
    private ValMapper[] valMappers;

    private static abstract class ValMapper {

        abstract void mapTo(Text settable, String[] vals);

    }

    private static final class PosValMapper extends ValMapper {
        private final int pos;

        PosValMapper(int pos) {
            this.pos = pos;
        }

        @Override
        void mapTo(Text settable, String[] vals) {
            settable.set(vals[pos]);
        }

        private static final Map<Integer, PosValMapper> cachedMappers = new HashMap<Integer, PosValMapper>(2);

        static final PosValMapper get(int val) {
            PosValMapper ret = cachedMappers.get(val);
            if (ret == null) {
                ret = new PosValMapper(val);
                cachedMappers.put(val, ret);
            }
            return ret;
        }

    }

    private static final class DefaultValMapper extends ValMapper {
        private final String val;

        private DefaultValMapper(String val) {
            this.val = val;
        }

        @Override
        void mapTo(Text settable, String[] vals) {
            settable.set(val);
        }

        private static final Map<String, DefaultValMapper> cachedMappers = new HashMap<String, DefaultValMapper>(2);

        static final DefaultValMapper get(String val) {
            DefaultValMapper ret = cachedMappers.get(val);
            if (ret == null) {
                ret = new DefaultValMapper(val);
                cachedMappers.put(val, ret);
            }
            return ret;
        }

    }

    public int setSpec(int pos, Text[] settables) {
        for (String specDataItem : specData) {
            settables[pos++].set(specDataItem);
        }
        return pos;
    }

    public int setVals(int pos, String[] vals, Text[] settables) {
        for (ValMapper valMapper : valMappers) {
            valMapper.mapTo(settables[pos++], vals);
        }
        return pos;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String specDatum : specData) {
            sb.append(specDatum).append("\t");
        }
        int i = 0;
        for (ValMapper mapper : valMappers) {
            if (mapper instanceof PosValMapper) {
                if (i++ > 0) {
                    sb.append(",");
                }
                sb.append(((PosValMapper) mapper).pos);
            }
        }
        return sb.toString();
    }

    public SegmentationSpec(String specRow, ColumnMapping[] colMap) {
        this.specData = new String[2];
        String[] cols = specRow.split("\t");
        this.specData[0] = cols[0];
        this.specData[1] = cols[1];
        String[] valStrs = cols[2].split(",");
        int[] vals = new int[valStrs.length];
        int i = 0;
        for (String valStr : valStrs) {
            vals[i++] = Integer.parseInt(valStr);
        }

        this.valMappers = new ValMapper[colMap.length];
        for (i = 0; i < colMap.length; i++) {
            if (Utils.containsArrayInt(vals, colMap[i].position())) {
                this.valMappers[i] = PosValMapper.get(colMap[i].position());
            } else {
                this.valMappers[i] = DefaultValMapper.get(colMap[i].defaultValue());
            }
        }

    }

}
