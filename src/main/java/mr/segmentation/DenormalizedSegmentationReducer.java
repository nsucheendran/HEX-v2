package mr.segmentation;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mr.aggregation.UserTransactionData;
import mr.aggregation.UserTransactionsAggregatedData;
import mr.dto.TextMultiple;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DenormalizedSegmentationReducer extends Reducer<TextMultiple, TextMultiple, NullWritable, TextMultiple> {
    private static BytesWritable bw = new BytesWritable(new byte[0], 0);
    private MultipleOutputs<NullWritable, TextMultiple> mos;
    private String outputDir;

    @Override
    public final void setup(final Context context) {
        mos = new MultipleOutputs<NullWritable, TextMultiple>(context);
        outputDir = context.getConfiguration().get("mapred.output.dir");
    }

    private String generateFileName(final Text variantCode, final Text experimentCode, final Text versionNum)
        throws UnsupportedEncodingException {
        String res = new StringBuilder().append(outputDir).append(Path.SEPARATOR).append("experiment_code=")
                .append(URLEncoder.encode(experimentCode.toString(), "UTF-8"))
                .append(Path.SEPARATOR).append("version_number=")
                .append(URLEncoder.encode(versionNum.toString(), "UTF-8"))
                .append(Path.SEPARATOR).append("variant_code=")
                .append(URLEncoder.encode(variantCode.toString(), "UTF-8"))
                .append(Path.SEPARATOR).append("/result").toString();
        return res;
    }

    @Override
    public final void reduce(final TextMultiple key, final Iterable<TextMultiple> values, final Context context) throws IOException,
            InterruptedException {
        Map<String, UserTransactionsAggregatedData> perUserTransactionData = new HashMap<String, UserTransactionsAggregatedData>();
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
        long numUniqueViewers = 0;
        long numRepeatPurchasers = 0;
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
        Set<Integer> excludes = new HashSet<Integer>() {
            /**
       * 
       */
            private static final long serialVersionUID = 1L;

            {
                add(2);
                add(3);
                add(4);
            }
        };
        /*
        mos.write(
                "outroot",
                bw,
                new Text(new TextMultiple(key, excludes, Long.toString(numUniqueViewers), Long.toString(numUniquePurchasers), Long
                        .toString(numUniqueCancellers), Long.toString(numActivePurchasers), Long.toString(numNilNetOrdersPurchasers), Long
                        .toString(numCancellations), Long.toString(netOrders), Double.toString(netBkgGBV), Long.toString(netBkgRoomNights),
                        Double.toString(netOmnitureGBV), Long.toString(netOmnitureRoomNights), Double.toString(netGrossProfit), Long
                                .toString(numRepeatPurchasers)).toString()),
                generateFileName(key.getTextElementAt(2), key.getTextElementAt(3), key.getTextElementAt(4)));
*/
    }

    @Override
    public final void cleanup(final Context context) throws IOException, InterruptedException {
        mos.close();
    }
}

