package mr.segmentation;

import java.io.IOException;
import java.util.UUID;

import mr.dto.TextMultiple;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SegmentationReducer extends Reducer<TextMultiple, TextMultiple, Text, NullWritable> {
    private final NullWritable bw = NullWritable.get();
    private final Text outText = new Text();
    private final StringBuilder outStr = new StringBuilder();
    private static final char SEP = (char) '\t';

    private long numUniqueViewers = 0;
    private long numUniquePurchasers = 0;
    private long numUniqueCancellers = 0;
    private long numActivePurchasers = 0;
    private long numNilNetOrdersPurchasers = 0;
    private long numCancellations = 0;
    private long netOrders = 0;
    private double netBkgGBV = 0;
    private long netBkgRoomNights = 0;
    private double netOmnitureGBV = 0;
    private long netOmnitureRoomNights = 0;
    private double netGrossProfit = 0;
    private long numRepeatPurchasers = 0;

    @Override
    public final void reduce(final TextMultiple key, final Iterable<TextMultiple> values, final Context context) throws IOException,
            InterruptedException {
        numUniqueViewers = 0;
        numUniquePurchasers = 0;
        numUniqueCancellers = 0;
        numActivePurchasers = 0;
        numNilNetOrdersPurchasers = 0;
        numCancellations = 0;
        netOrders = 0;
        netBkgGBV = 0;
        netBkgRoomNights = 0;
        netOmnitureGBV = 0;
        netOmnitureRoomNights = 0;
        netGrossProfit = 0;
        numRepeatPurchasers = 0;

        for (TextMultiple value : values) {
            numUniqueViewers += Long.parseLong(value.getTextElementAt(0).toString());
            numUniquePurchasers += Long.parseLong(value.getTextElementAt(1).toString());
            numUniqueCancellers += Long.parseLong(value.getTextElementAt(2).toString());
            numActivePurchasers += Long.parseLong(value.getTextElementAt(3).toString());
            numNilNetOrdersPurchasers += Long.parseLong(value.getTextElementAt(4).toString());
            numCancellations += Long.parseLong(value.getTextElementAt(5).toString());
            netOrders += Long.parseLong(value.getTextElementAt(6).toString());
            netBkgGBV += Double.parseDouble(value.getTextElementAt(7).toString());
            netBkgRoomNights += Long.parseLong(value.getTextElementAt(8).toString());
            netOmnitureGBV += Double.parseDouble(value.getTextElementAt(9).toString());
            netOmnitureRoomNights += Long.parseLong(value.getTextElementAt(10).toString());
            netGrossProfit += Double.parseDouble(value.getTextElementAt(11).toString());
            numRepeatPurchasers += Long.parseLong(value.getTextElementAt(12).toString());

        }
        outStr.setLength(0);
        outStr.append(UUID.randomUUID().toString()).append(SEP);
        key.toStringBuilderSep(outStr, SEP, key.size() - 3, key.size() - 2, key.size() - 1);
        outStr.append(SEP).append(numUniqueViewers).append(SEP).append(numUniquePurchasers).append(SEP).append(numUniqueCancellers)
                .append(SEP).append(numActivePurchasers).append(SEP).append(numNilNetOrdersPurchasers).append(SEP).append(numCancellations)
                .append(SEP).append(netOrders).append(SEP).append(netBkgGBV).append(SEP).append(netBkgRoomNights).append(SEP)
                .append(netOmnitureGBV).append(SEP).append(netOmnitureRoomNights).append(SEP).append(netGrossProfit).append(SEP)
                .append(numRepeatPurchasers);

        outStr.append(SEP).append(key.getTextElementAt(key.size() - 3).toString()).append(SEP)
                .append(key.getTextElementAt(key.size() - 2).toString()).append(SEP)
                .append(key.getTextElementAt(key.size() - 1).toString());
        outText.set(outStr.toString());
        context.write(outText, bw);
    }
    
}
