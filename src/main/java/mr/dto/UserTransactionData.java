package mr.dto;

import static mr.utils.Utils.coalesce;

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
   * guid => 0 itin_number => 1 trans_date => 2 num_transactions => 3 bkg_gbv => 4 bkg_room_nights => 5 omniture_gbv => 6
   * omniture_room_nights => 7 gross_profit => 8
   */
  public UserTransactionData(TextMultiple mapperOutput) {
    itinNumber = coalesce(mapperOutput.getTextElementAt(1).toString(), "");
    transDate = coalesce(mapperOutput.getTextElementAt(2).toString(), "");
    numTrans = Integer.parseInt(coalesce(mapperOutput.getTextElementAt(3).toString(), "0"));
    bkgGbv = Integer.parseInt(coalesce(mapperOutput.getTextElementAt(4).toString(), "0"));
    bkgRoomNights = Integer.parseInt(coalesce(mapperOutput.getTextElementAt(5).toString(), "0"));
    omnitureGbv = Integer.parseInt(coalesce(mapperOutput.getTextElementAt(6).toString(), "0"));
    omnitureRoomNights = Integer.parseInt(coalesce(mapperOutput.getTextElementAt(7).toString(), "0"));
    grossProfit = Double.parseDouble(coalesce(mapperOutput.getTextElementAt(8).toString(), "0"));
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

  public double getOmnitureGbv() {
    return omnitureGbv;
  }

  public void setOmnitureGbv(double omnitureGbv) {
    this.omnitureGbv = omnitureGbv;
  }

  public int getOmnitureRoomNights() {
    return omnitureRoomNights;
  }

  public void setOmnitureRoomNights(int omnitureRoomNights) {
    this.omnitureRoomNights = omnitureRoomNights;
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

  public void setItinNumber(String itinNumber) {
    this.itinNumber = itinNumber;
  }
}
