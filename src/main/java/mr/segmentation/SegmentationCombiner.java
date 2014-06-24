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

import org.apache.hadoop.mapreduce.Reducer;

import com.expedia.edw.hww.hex.etl.dto.TextMultiple;

public class SegmentationCombiner extends Reducer<TextMultiple, TextMultiple, TextMultiple, TextMultiple> {
  private final TextMultiple outVal = new TextMultiple(new String[13]);

  @Override
  public final void reduce(final TextMultiple key, final Iterable<TextMultiple> values, final Context context)
    throws IOException, InterruptedException {
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

    outVal.getTextElementAt(NUM_UNIQUE_VIEWERS).set(Long.toString(numUniqueViewers));
    outVal.getTextElementAt(NUM_UNIQUE_PURCHASERS).set(Long.toString(numUniquePurchasers));
    outVal.getTextElementAt(NUM_UNIQUE_CANCELLERS).set(Long.toString(numUniqueCancellers));
    outVal.getTextElementAt(NUM_ACTIVE_PURCHASERS).set(Long.toString(numActivePurchasers));
    outVal.getTextElementAt(NUM_INACTIVE_PURCHASERS).set(Long.toString(numNilNetOrdersPurchasers));
    outVal.getTextElementAt(NUM_CANCELLATIONS).set(Long.toString(numCancellations));
    outVal.getTextElementAt(NET_ORDERS).set(Long.toString(netOrders));
    outVal.getTextElementAt(NET_BKG_GBV).set(Double.toString(netBkgGBV));
    outVal.getTextElementAt(NET_BKG_ROOM_NIGHTS).set(Long.toString(netBkgRoomNights));
    outVal.getTextElementAt(NET_OMNITURE_GBV).set(Double.toString(netOmnitureGBV));
    outVal.getTextElementAt(NET_OMNITURE_ROOM_NIGHTS).set(Long.toString(netOmnitureRoomNights));
    outVal.getTextElementAt(NET_GROSS_PROFIT).set(Double.toString(netGrossProfit));
    outVal.getTextElementAt(NUM_REPEAT_PURCHASERS).set(Long.toString(numRepeatPurchasers));

    context.write(key, outVal);
  }

}
