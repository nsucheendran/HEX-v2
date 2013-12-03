package mr.aggregation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import mr.dto.TextMultiple;
import mr.dto.UserTransactionData;
import mr.dto.UserTransactionsAggregatedData;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class R4Reducer extends Reducer<TextMultiple, TextMultiple, NullWritable, TextMultiple> {
    private MultipleOutputs mos;
    private String outputDir;

    public void setup(Context context) {
        mos = new MultipleOutputs(context);
        outputDir = context.getConfiguration().get("mapred.output.dir");
    }

    private String generateFileName(Text experimentCode, Text variantCode, Text versionNum) {
        Path vcPath = new Path(outputDir, new Path("variant_code=" + variantCode));
        Path ecPath = new Path(vcPath, new Path("experiment_code=" + experimentCode));
        Path vnPath = new Path(ecPath, new Path("version_number=" + versionNum));
        Path finalPath = new Path(vnPath, new Path("result"));
        return finalPath.toString();
    }

    @Override
    public void reduce(TextMultiple key, Iterable<TextMultiple> values, Context context) throws IOException, InterruptedException {
        Map<Text, UserTransactionsAggregatedData> perUserTransactionData = new HashMap<Text, UserTransactionsAggregatedData>();
        long numUniquePurchasers = 0;
        long numUniqueCancellers = 0;
        long numActivePurchasers = 0;
        long numNilNetOrdersPurchasers = 0;
        long numCancellations = 0;
        long netOrders = 0;
        double netGBV = 0;
        long netRoomNights = 0;
        double netGrossProfit = 0;
        long numUniqueViewers = 0;
        long numRepeatPurchasers = 0;
        /*
         * guid => 0 itin_number => 1 trans_date => 2 num_transactions => 3 bkg_gbv => 4 bkg_room_nights => 5 omniture_gbv => 6
         * omniture_room_nights => 7 gross_profit => 8
         */
        for (TextMultiple value : values) {
            Text guid = value.getTextElementAt(0);
            UserTransactionData newTransData = new UserTransactionData(value);
            UserTransactionsAggregatedData userAggTransData = perUserTransactionData.get(guid);
            if (userAggTransData == null) {
                userAggTransData = new UserTransactionsAggregatedData();
                numUniqueViewers++;
            }
            boolean isAlreadyAPurchaser = userAggTransData.isPurchaser();
            boolean isAlreadyACanceller = userAggTransData.isCanceller();
            userAggTransData.addTransaction(newTransData);
            if (!isAlreadyAPurchaser && userAggTransData.isPurchaser()) {
                numUniquePurchasers++;
            }
            if (!isAlreadyACanceller && userAggTransData.isCanceller()) {
                numUniqueCancellers++;
            }
        }
        for (Map.Entry<Text, UserTransactionsAggregatedData> userAggTransData : perUserTransactionData.entrySet()) {
            if (userAggTransData.getValue().isRepeatPurchaser()) {
                numRepeatPurchasers++;
            }
            if (userAggTransData.getValue().getNetTransactions() > 0) {
                numActivePurchasers++;
            }
            if (userAggTransData.getValue().isPurchaser() && userAggTransData.getValue().getNetTransactions() == 0) {
                numNilNetOrdersPurchasers++;
            }
            numCancellations += userAggTransData.getValue().isNumCancellations();
            netOrders += userAggTransData.getValue().getNetOrders();
            netGBV += userAggTransData.getValue().getNetGBV();
            netRoomNights += userAggTransData.getValue().getNetRoomNights();
            netGrossProfit += userAggTransData.getValue().getNetGrossProfit();
        }
        System.out.println("key>>>>>" + key);
        mos.write(
                NullWritable.get(),
                new TextMultiple(key, Long.toString(numUniqueViewers), Long.toString(numUniquePurchasers), Long
                        .toString(numUniqueCancellers), Long.toString(numActivePurchasers), Long.toString(numNilNetOrdersPurchasers), Long
                        .toString(numCancellations), Long.toString(netOrders), Double.toString(netGBV), Long.toString(netRoomNights),
                /* omnitureGBV and omnitureRoomNights? */
                Double.toString(netGrossProfit), Long.toString(numRepeatPurchasers)),
                generateFileName(key.getTextElementAt(2), key.getTextElementAt(3), key.getTextElementAt(4)));
        /*
         * context.write( NullWritable.get(), new TextMultiple(key, Long.toString(numUniqueViewers), Long.toString(numUniquePurchasers),
         * Long .toString(numUniqueCancellers), Long.toString(numActivePurchasers), Long.toString(numNilNetOrdersPurchasers),
         * Long.toString(numCancellations), Long.toString(netOrders), Double.toString(netGBV), Long.toString(netRoomNights),
         * 
         * Double.toString(netGrossProfit), Long.toString(numRepeatPurchasers)));
         */
    }
    
    public void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
