package mr.dto;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class UserTransactionsAggregatedData {
    private Map<String, AggItineraryData> allTransDataByItinNumber;
    private int netTransactions;
    private double totalBkgGbv;
    private int totalBkgRoomNights;
    private double totalOmnitureGbv;
    private int totalOmnitureRoomNights;
    private boolean isPurchaser;
    private boolean isCanceller;
    private long numCancellations;
    private double netGrossProfit = 0;

    public int getNetTransactions() {
        return netTransactions;
    }

    public void setNetTransactions(int totalNumTrans) {
        this.netTransactions = totalNumTrans;
    }

    public double getTotalBkgGbv() {
        return totalBkgGbv;
    }

    public int getTotalBkgRoomNights() {
        return totalBkgRoomNights;
    }

    public double getTotalOmnitureGbv() {
        return totalOmnitureGbv;
    }

    public int getTotalOmnitureRoomNights() {
        return totalOmnitureRoomNights;
    }

    public boolean isPurchaser() {
        return isPurchaser;
    }

    public long getNumCancellations() {
        return numCancellations;
    }

    public double getNetGrossProfit() {
        return netGrossProfit;
    }

    public boolean isCanceller() {
        return isCanceller;
    }

    public void addTransaction(UserTransactionData newTransData) {
        if (allTransDataByItinNumber == null) {
            allTransDataByItinNumber = new HashMap<String, AggItineraryData>(
                    2000);
        }
        AggItineraryData aggTransByItinNumber = allTransDataByItinNumber.get(newTransData.getItinNumber());
        if (aggTransByItinNumber == null) {
            aggTransByItinNumber = new AggItineraryData();
        }
        if (newTransData.getNumTrans() != 0) {
            aggTransByItinNumber.addToNumTrans(newTransData.getNumTrans());
            aggTransByItinNumber.setMinTransDate(newTransData.getTransDate());
            allTransDataByItinNumber.put(newTransData.getItinNumber(), aggTransByItinNumber);
        }
        totalBkgGbv += newTransData.getBkgGbv();
        totalBkgRoomNights += newTransData.getBkgRoomNights();
        totalOmnitureGbv += newTransData.getOmnitureGbv();
        totalOmnitureRoomNights += newTransData.getOmnitureRoomNights();
        netTransactions += newTransData.getNumTrans();
        if (!isPurchaser && newTransData.getNumTrans() > 0) {
            isPurchaser = true;
        }
        if (newTransData.getNumTrans() < 0) {
            isCanceller = true;
            numCancellations += (0 - newTransData.getNumTrans());
        }
        netGrossProfit += newTransData.getGrossProfit();
    }

    public boolean isRepeatPurchaser() {
        boolean isRepeatPurchaser = false;
        String firstPurchaseDate = null;
        for (Map.Entry<String, AggItineraryData> aggTransByItinNumber : allTransDataByItinNumber.entrySet()) {
            if (aggTransByItinNumber.getValue().getNumTrans() > 0) {
                if (firstPurchaseDate == null && StringUtils.isNotEmpty(aggTransByItinNumber.getValue().getMinTransDate())) {
                    firstPurchaseDate = aggTransByItinNumber.getValue().getMinTransDate();
                } else if (!aggTransByItinNumber.getValue().getMinTransDate().equals(firstPurchaseDate)) {
                    isRepeatPurchaser = true;
                    break;
                }
            }
        }
        return isRepeatPurchaser;
    }

    static class AggItineraryData {
        private int numTrans;
        private String minTransDate;

        public int getNumTrans() {
            return numTrans;
        }

        public void addToNumTrans(int numTrans) {
            this.numTrans += numTrans;
        }

        public String getMinTransDate() {
            return minTransDate;
        }

        public void setMinTransDate(String transDate) {
            if (this.minTransDate == null || transDate.compareTo(minTransDate) < 0) {
                this.minTransDate = transDate;
            }
        }
    }
}

