package mr.aggregation;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
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
    private MultipleOutputs<NullWritable, TextMultiple> mos;
    private String outputDir;

    public void setup(Context context) {
        mos = new MultipleOutputs<NullWritable, TextMultiple>(context);
        outputDir = context.getConfiguration().get("mapred.output.dir");

    }

    private String generateFileName(Text experimentCode, Text variantCode, Text versionNum) throws UnsupportedEncodingException {
        String res = new StringBuilder().append(outputDir).append(Path.SEPARATOR).append("variant_code=")
                .append(URLEncoder.encode(variantCode.toString(), "UTF-8")).append(Path.SEPARATOR).append("experiment_code=")
                .append(URLEncoder.encode(experimentCode.toString(), "UTF-8")).append(Path.SEPARATOR).append("version_number=")
                .append(URLEncoder.encode(versionNum.toString(), "UTF-8")).append(Path.SEPARATOR).append("/result").toString();
        
        return res;
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
        // System.out.println("key>>>>>" + key);
        mos.write(
                "outroot",
                NullWritable.get(),
                new TextMultiple(key, Long.toString(numUniqueViewers), Long.toString(numUniquePurchasers), Long
                        .toString(numUniqueCancellers), Long.toString(numActivePurchasers), Long.toString(numNilNetOrdersPurchasers), Long
                        .toString(numCancellations), Long.toString(netOrders), Double.toString(netGBV), Long.toString(netRoomNights),
                /* omnitureGBV and omnitureRoomNights? */
                Double.toString(netGrossProfit), Long.toString(numRepeatPurchasers)),
                generateFileName(key.getTextElementAt(2), key.getTextElementAt(3), key.getTextElementAt(4)));

    }
    
    public void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
