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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class R4Reducer extends Reducer<TextMultiple, TextMultiple, BytesWritable, Text> {
    private final BytesWritable bw = new BytesWritable(new byte[0], 0);
    private final Text outText = new Text();
    private final StringBuilder outStr = new StringBuilder();

    //private MultipleOutputs<BytesWritable, Text> mos;
    private String outputDir;

    // exclude values for columns in the following positions from the output
    private final int[] excludes = new int[] { 2, 3, 4 };

    @Override
    public final void setup(final Context context) {
        //mos = new MultipleOutputs<BytesWritable, Text>(context);
        outputDir = context.getConfiguration().get("mapred.output.dir");
    }

    private String generateFileName(final Text variantCode, final Text experimentCode, final Text versionNum)
            throws UnsupportedEncodingException {
        String res = new StringBuilder().append(outputDir).append(Path.SEPARATOR).append("experiment_code=")
                .append(URLEncoder.encode(experimentCode.toString(), "UTF-8")).append(Path.SEPARATOR).append("version_number=")
                .append(URLEncoder.encode(versionNum.toString(), "UTF-8")).append(Path.SEPARATOR).append("variant_code=")
                .append(URLEncoder.encode(variantCode.toString(), "UTF-8")).append(Path.SEPARATOR).append("/result").toString();
        return res;
    }

    private Map<String, UserTransactionsAggregatedData> perUserTransactionData;
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
    private long numUniqueViewers = 0;
    private long numRepeatPurchasers = 0;

    @Override
    public final void reduce(final TextMultiple key, final Iterable<TextMultiple> values, final Context context) throws IOException,
            InterruptedException {
        perUserTransactionData = new HashMap<String, UserTransactionsAggregatedData>(5000);
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
        numUniqueViewers = 0;
        numRepeatPurchasers = 0;
        /*
         * guid => 0 itin_number => 1 trans_date => 2 num_transactions => 3 bkg_gbv => 4 bkg_room_nights => 5 omniture_gbv => 6
         * omniture_room_nights => 7 gross_profit => 8
         */
        for (TextMultiple value : values) {
            Text guid = value.getTextElementAt(0);
            UserTransactionData newTransData = new UserTransactionData(value);
            UserTransactionsAggregatedData userAggTransData = perUserTransactionData.get(guid.toString());
            if (userAggTransData == null) {
                userAggTransData = new UserTransactionsAggregatedData();
                numUniqueViewers++;
                perUserTransactionData.put(guid.toString(), userAggTransData);
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
        for (Map.Entry<String, UserTransactionsAggregatedData> userAggTransData : perUserTransactionData.entrySet()) {
            if (userAggTransData.getValue().isRepeatPurchaser()) {
                numRepeatPurchasers++;
            }
            if (userAggTransData.getValue().getNetTransactions() > 0) {
                numActivePurchasers++;
            }
            if (userAggTransData.getValue().isPurchaser() && userAggTransData.getValue().getNetTransactions() == 0) {
                numNilNetOrdersPurchasers++;
            }
            numCancellations += userAggTransData.getValue().getNumCancellations();
            netOrders += userAggTransData.getValue().getNetTransactions();
            netBkgGBV += userAggTransData.getValue().getTotalBkgGbv();
            netBkgRoomNights += userAggTransData.getValue().getTotalBkgRoomNights();
            netOmnitureGBV += userAggTransData.getValue().getTotalOmnitureGbv();
            netOmnitureRoomNights += userAggTransData.getValue().getTotalOmnitureRoomNights();
            netGrossProfit += userAggTransData.getValue().getNetGrossProfit();
        }
        outStr.setLength(0);
        key.toStringBuilder(excludes,
                outStr.append(numUniqueViewers).append(SEP).append(numUniquePurchasers).append(SEP).append(numUniqueCancellers).append(SEP)
                        .append(numActivePurchasers).append(SEP).append(numNilNetOrdersPurchasers).append(SEP).append(numCancellations)
                        .append(SEP).append(netOrders).append(SEP).append(netBkgGBV).append(SEP).append(netBkgRoomNights).append(SEP)
                        .append(netOmnitureGBV).append(SEP).append(netOmnitureRoomNights).append(SEP).append(netGrossProfit).append(SEP)
                        .append(numRepeatPurchasers).append(SEP));
        outStr.append(SEP).append(key.getTextElementAt(3).toString()).append(SEP).append(key.getTextElementAt(4).toString())
                .append(SEP).append(key.getTextElementAt(4).toString());
        outText.set(outStr.toString());
        context.write(bw, outText);
        //mos.write("outroot", bw, outText, generateFileName(key.getTextElementAt(2), key.getTextElementAt(3), key.getTextElementAt(4)));

    }

    private static final char SEP = (char) 1;
/*
    @Override
    public final void cleanup(final Context context) throws IOException, InterruptedException {
        // mos.close();
    }
    */
}
