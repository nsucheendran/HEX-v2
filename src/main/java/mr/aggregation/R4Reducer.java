package mr.aggregation;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

import mr.dto.TextMultiple;
import mr.dto.UserTransactionData;
import mr.dto.UserTransactionsAggregatedData;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class R4Reducer extends MapReduceBase implements Reducer<TextMultiple, TextMultiple, TextMultiple, TextMultiple> {
	
	public void reduce(TextMultiple key, Iterator<TextMultiple> values, OutputCollector<TextMultiple, TextMultiple> output, Reporter reporter)
			throws IOException {
		Map<Text, UserTransactionsAggregatedData> perUserTransactionData = new HashMap<Text, UserTransactionsAggregatedData>();
		int numUniquePurchasers = 0;
		int numUniqueCancellers = 0;
		int numActivePurchasers = 0;
		int numNilNetOrdersPurchasers = 0;
		long numCancellations = 0;
		long netOrders = 0;
		double netGBV = 0;
		long netRoomNights = 0;
		double netGrossProfit = 0;
		int numUniqueViewers = 0;
		int numRepeatPurchasers = 0;
		
		while(values.hasNext()) {
			TextMultiple value = values.next();
			Text guid = value.getTextElementAt(0);
			UserTransactionData newTransData = new UserTransactionData(value);
			UserTransactionsAggregatedData userAggTransData = perUserTransactionData.get(guid);
			if(userAggTransData == null) {
				userAggTransData = new UserTransactionsAggregatedData();
				numUniqueViewers++;
			}
			boolean isAlreadyAPurchaser = userAggTransData.isPurchaser();
			boolean isAlreadyACanceller = userAggTransData.isCanceller();
			userAggTransData.addTransaction(newTransData);
			if(!isAlreadyAPurchaser && userAggTransData.isPurchaser()) {
				numUniquePurchasers++;
			}
			if(!isAlreadyACanceller && userAggTransData.isCanceller()) {
				numUniqueCancellers++;
			}	
		}
		for(Map.Entry<Text, UserTransactionsAggregatedData> userAggTransData: perUserTransactionData.entrySet()) {
			if(userAggTransData.getValue().isRepeatPurchaser()) {
				numRepeatPurchasers++;
			}
			if(userAggTransData.getValue().getNetTransactions()>0) {
				numActivePurchasers++;
			}
			if(userAggTransData.getValue().isPurchaser() && userAggTransData.getValue().getNetTransactions()==0) {
				numNilNetOrdersPurchasers++;
			}
			numCancellations+=userAggTransData.getValue().isNumCancellations();
			netOrders += userAggTransData.getValue().getNetOrders();
			netGBV += userAggTransData.getValue().getNetGBV();
			netRoomNights += userAggTransData.getValue().getNetRoomNights();
			netGrossProfit += userAggTransData.getValue().getNetGrossProfit();
		}
		output.collect(key, new TextMultiple(Integer.toString(numUniqueViewers), Integer.toString(numUniquePurchasers), Integer.toString(numUniqueCancellers), 
				Integer.toString(numActivePurchasers), Integer.toString(numNilNetOrdersPurchasers), Long.toString(numCancellations), Long.toString(netOrders),
				Double.toString(netGBV), Long.toString(netRoomNights), Double.toString(netGrossProfit), Integer.toString(numRepeatPurchasers)));
	}
}
