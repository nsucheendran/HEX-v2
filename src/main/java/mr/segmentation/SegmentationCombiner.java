package mr.segmentation;

import java.io.IOException;

import mr.dto.TextMultiple;

import org.apache.hadoop.mapreduce.Reducer;

public class SegmentationCombiner extends Reducer<TextMultiple, TextMultiple, TextMultiple, TextMultiple> {
    private final TextMultiple outVal = new TextMultiple(new String[13]);
    
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
        
        outVal.getTextElementAt(0).set(Long.toString(numUniqueViewers));
        outVal.getTextElementAt(1).set(Long.toString(numUniquePurchasers));
        outVal.getTextElementAt(2).set(Long.toString(numUniqueCancellers));
        outVal.getTextElementAt(3).set(Long.toString(numActivePurchasers));
        outVal.getTextElementAt(4).set(Long.toString(numNilNetOrdersPurchasers));
        outVal.getTextElementAt(5).set(Long.toString(numCancellations));
        outVal.getTextElementAt(6).set(Long.toString(netOrders));
        outVal.getTextElementAt(7).set(Double.toString(netBkgGBV));
        outVal.getTextElementAt(8).set(Long.toString(netBkgRoomNights));
        outVal.getTextElementAt(9).set(Double.toString(netOmnitureGBV));
        outVal.getTextElementAt(10).set(Long.toString(netOmnitureRoomNights));
        outVal.getTextElementAt(11).set(Double.toString(netGrossProfit));
        outVal.getTextElementAt(12).set(Long.toString(numRepeatPurchasers));
        
        context.write(key, outVal);
    }

}
