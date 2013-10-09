package mr.dto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserTransactionsAggregatedData {
	private Map<String, List<UserTransactionData>> allTransDataByItinNumber;
	private int netTransactions;
	private double totalBkgGbv;
	private int totalBkgRoomNights;
	private boolean isPurchaser;
	private boolean isCanceller;
	private long numCancellations;
	private long netOrders;
	private double netGBV = 0;
	private long netRoomNights = 0;
	private double netGrossProfit = 0;
	
	public Map<String, List<UserTransactionData>> getAllTransData() {
		return allTransDataByItinNumber;
	}
	public int getNetTransactions() {
		return netTransactions;
	}
	public void setNetTransactions(int totalNumTrans) {
		this.netTransactions = totalNumTrans;
	}
	public double getTotalBkgGbv() {
		return totalBkgGbv;
	}
	public void setTotalBkgGbv(double totalBkgGbv) {
		this.totalBkgGbv = totalBkgGbv;
	}
	public int getTotalBkgRoomNights() {
		return totalBkgRoomNights;
	}
	public void setTotalBkgRoomNights(int totalBkgRoomNights) {
		this.totalBkgRoomNights = totalBkgRoomNights;
	}
	public boolean isPurchaser() {
		return isPurchaser;
	}
	public long isNumCancellations() {
		return numCancellations;
	}
	public long getNetOrders() {
		return netOrders;
	}
	public double getNetGBV() {
		return netGBV;
	}
	public long getNetRoomNights() {
		return netRoomNights;
	}
	public double getNetGrossProfit() {
		return netGrossProfit;
	}
	public boolean isCanceller() {
		return isCanceller;
	}
	
	public void addTransaction(UserTransactionData newTransData) {
		if(allTransDataByItinNumber == null) {
			allTransDataByItinNumber = new HashMap<String, List<UserTransactionData>>();
		}
		List<UserTransactionData> userTransByItinNumber = allTransDataByItinNumber.get(newTransData.getItinNumber());
		if (newTransData.getNumTrans() < 0) {
			if(userTransByItinNumber!=null) {
				for(UserTransactionData userTransData: userTransByItinNumber) {
					if(userTransData.getNumTrans()>0) {
						userTransData.setCancelled(true);
					}
				}
			}
		}
		if(userTransByItinNumber == null) {
			userTransByItinNumber = new ArrayList<UserTransactionData>();
		}
		userTransByItinNumber.add(newTransData);
		allTransDataByItinNumber.put(newTransData.getItinNumber(), userTransByItinNumber);
		totalBkgGbv += newTransData.getBkgGbv();
		totalBkgRoomNights += newTransData.getBkgRoomNights();
		netTransactions += newTransData.getNumTrans();
		if(!isPurchaser && newTransData.getNumTrans()>0) {
			isPurchaser = true;
		}
		if(newTransData.getNumTrans()<0) {
			isCanceller = true;
			numCancellations += newTransData.getNumTrans();
		}
		netOrders += newTransData.getNumTrans();
		netGBV += newTransData.getBkgGbv();
		netRoomNights += newTransData.getBkgRoomNights();
		netGrossProfit += newTransData.getGrossProfit();
	}
	public boolean isRepeatPurchaser() {
		boolean isRepeatPurchaser = false;
		String firstPurchaseDate = null;
		for(Map.Entry<String, List<UserTransactionData>> transByItinNumber: allTransDataByItinNumber.entrySet()) {
			for(UserTransactionData trans: transByItinNumber.getValue()) {
				if(!trans.isCancelled() && trans.getNumTrans()>0) {
					if (firstPurchaseDate == null) {
						firstPurchaseDate = trans.getTransDate();
						break;
					} else if (!trans.getTransDate().equals(firstPurchaseDate)) {
						isRepeatPurchaser = true;
						break;
					}
				}
			}
		}
		return isRepeatPurchaser;
	}
}
