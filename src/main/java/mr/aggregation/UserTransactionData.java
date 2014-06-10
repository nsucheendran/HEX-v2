package mr.aggregation;

import static mr.utils.Utils.coalesce;
import mr.dto.TextMultiple;

public class UserTransactionData {
	private String transDate;
	private int numTrans;
	private double bkgGbv;
	private int bkgRoomNights;
	private double omnitureGbv;
	private int omnitureRoomNights;
	private double grossProfit;
	private String itinNumber;

	/*
	 * guid => 0 itin_number => 1 trans_date => 2 num_transactions => 3 bkg_gbv
	 * => 4 bkg_room_nights => 5 omniture_gbv => 6 omniture_room_nights => 7
	 * gross_profit => 8
	 */
	public UserTransactionData(TextMultiple mapperOutput) {
		itinNumber = coalesce(mapperOutput.getTextElementAt(1).toString(), "");
		transDate = coalesce(mapperOutput.getTextElementAt(2).toString(), "");
		numTrans = Integer.parseInt(coalesce(mapperOutput.getTextElementAt(3)
				.toString(), "0"));
		bkgGbv = Double.parseDouble(coalesce(mapperOutput.getTextElementAt(4)
				.toString(), "0"));
		bkgRoomNights = Integer.parseInt(coalesce(mapperOutput
				.getTextElementAt(5).toString(), "0"));
		omnitureGbv = Double.parseDouble(coalesce(mapperOutput
				.getTextElementAt(6).toString(), "0"));
		omnitureRoomNights = Integer.parseInt(coalesce(mapperOutput
				.getTextElementAt(7).toString(), "0"));
		grossProfit = Double.parseDouble(coalesce(mapperOutput
				.getTextElementAt(8).toString(), "0"));
	}

	public String getTransDate() {
		return transDate;
	}

	public int getNumTrans() {
		return numTrans;
	}

	public double getBkgGbv() {
		return bkgGbv;
	}

	public int getBkgRoomNights() {
		return bkgRoomNights;
	}

	public double getOmnitureGbv() {
		return omnitureGbv;
	}

	public int getOmnitureRoomNights() {
		return omnitureRoomNights;
	}

	public double getGrossProfit() {
		return grossProfit;
	}

	public String getItinNumber() {
		return itinNumber;
	}
}
