/*
 * @author achadha
 */

package mr.segmentation;

import static mr.segmentation.MeasureFieldPositions.NET_BKG_GBV;
import static mr.segmentation.MeasureFieldPositions.NET_BKG_ROOM_NIGHTS;
import static mr.segmentation.MeasureFieldPositions.NET_GROSS_PROFIT;
import static mr.segmentation.MeasureFieldPositions.NET_OMNITURE_GBV;
import static mr.segmentation.MeasureFieldPositions.NET_OMNITURE_ROOM_NIGHTS;
import static mr.segmentation.MeasureFieldPositions.NET_ORDERS;
import static mr.segmentation.MeasureFieldPositions.NUM_ACTIVE_PURCHASERS;
import static mr.segmentation.MeasureFieldPositions.NUM_CANCELLATIONS;
import static mr.segmentation.MeasureFieldPositions.NUM_INACTIVE_PURCHASERS;
import static mr.segmentation.MeasureFieldPositions.NUM_REPEAT_PURCHASERS;
import static mr.segmentation.MeasureFieldPositions.NUM_UNIQUE_CANCELLERS;
import static mr.segmentation.MeasureFieldPositions.NUM_UNIQUE_PURCHASERS;
import static mr.segmentation.MeasureFieldPositions.NUM_UNIQUE_VIEWERS;

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

    @Override
    public final void reduce(final TextMultiple key, final Iterable<TextMultiple> values, final Context context) throws IOException,
            InterruptedException {
        long numUniqueViewers = 0;
        long numUniquePurchasers = 0;
        long numUniqueCancellers = 0;
        long numActivePurchasers = 0;
        long numNilNetOrdersPurchasers = 0;
        long numCancellations = 0;
        long netOrders = 0;
        double netBkgGBV = 0;
        long netBkgRoomNights = 0;
        double netOmnitureGBV = 0;
        long netOmnitureRoomNights = 0;
        double netGrossProfit = 0;
        long numRepeatPurchasers = 0;

        for (TextMultiple value : values) {
            numUniqueViewers += Long.parseLong(value.getTextElementAt(NUM_UNIQUE_VIEWERS).toString());
            numUniquePurchasers += Long.parseLong(value.getTextElementAt(NUM_UNIQUE_PURCHASERS).toString());
            numUniqueCancellers += Long.parseLong(value.getTextElementAt(NUM_UNIQUE_CANCELLERS).toString());
            numActivePurchasers += Long.parseLong(value.getTextElementAt(NUM_ACTIVE_PURCHASERS).toString());
            numNilNetOrdersPurchasers += Long.parseLong(value.getTextElementAt(NUM_INACTIVE_PURCHASERS).toString());
            numCancellations += Long.parseLong(value.getTextElementAt(NUM_CANCELLATIONS).toString());
            netOrders += Long.parseLong(value.getTextElementAt(NET_ORDERS).toString());
            netBkgGBV += Double.parseDouble(value.getTextElementAt(NET_BKG_GBV).toString());
            netBkgRoomNights += Long.parseLong(value.getTextElementAt(NET_BKG_ROOM_NIGHTS).toString());
            netOmnitureGBV += Double.parseDouble(value.getTextElementAt(NET_OMNITURE_GBV).toString());
            netOmnitureRoomNights += Long.parseLong(value.getTextElementAt(NET_OMNITURE_ROOM_NIGHTS).toString());
            netGrossProfit += Double.parseDouble(value.getTextElementAt(NET_GROSS_PROFIT).toString());
            numRepeatPurchasers += Long.parseLong(value.getTextElementAt(NUM_REPEAT_PURCHASERS).toString());

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
