package mr.dto;
import static mr.Constants.*;

public class UserTransactionData {
	private String transDate;
	private int numTrans;
	private double bkgGbv;
	private int bkgRoomNights;
	private double grossProfit; 
	private String itinNumber;
	private boolean isCancelled;

	public UserTransactionData(TextMultiple mapperOutput) {
		if((transDate = mapperOutput.getTextElementAt(1).toString()).equals(HIVE_NULL_VALUE)) {
			transDate = "";
		}
		String val = null;
		if((val=mapperOutput.getTextElementAt(2).toString()).equals(HIVE_NULL_VALUE)) {
			numTrans = 0;
		} else {
			numTrans = Integer.parseInt(val);
		}
		if((val=mapperOutput.getTextElementAt(3).toString()).equals(HIVE_NULL_VALUE)) {
			bkgGbv = 0;
		} else {
			bkgGbv = Double.parseDouble(val);
		}
		if((val=mapperOutput.getTextElementAt(4).toString()).equals(HIVE_NULL_VALUE)) {
			bkgRoomNights = 0;
		} else {
			bkgRoomNights = Integer.parseInt(val);
		}
		if((val=mapperOutput.getTextElementAt(5).toString()).equals(HIVE_NULL_VALUE)) {
			grossProfit = 0;
		} else {
			grossProfit = Double.parseDouble(val);
		}
	}
	
	public String getTransDate() {
		return transDate;
	}
	public void setTransDate(String transDate) {
		this.transDate = transDate;
	}
	public int getNumTrans() {
		return numTrans;
	}
	public void setNumTrans(int numTrans) {
		this.numTrans = numTrans;
	}
	public double getBkgGbv() {
		return bkgGbv;
	}
	public void setBkgGbv(double bkgGbv) {
		this.bkgGbv = bkgGbv;
	}
	public int getBkgRoomNights() {
		return bkgRoomNights;
	}
	public void setBkgRoomNights(int bkgRoomNights) {
		this.bkgRoomNights = bkgRoomNights;
	}
	public double getGrossProfit() {
		return grossProfit;
	}
	public void setGrossProfit(double grossProfit) {
		this.grossProfit = grossProfit;
	}
	public String getItinNumber() {
		return itinNumber;
	}
	public void setItinNumber(String itin_number) {
		this.itinNumber = itin_number;
	}
	public boolean isCancelled() {
		return isCancelled;
	}
	public void setCancelled(boolean isCancelled) {
		this.isCancelled = isCancelled;
	}
}
