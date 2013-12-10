package mr;

import java.util.regex.Pattern;

public interface Constants {
  /**
   * These field positions will be read from Hive Metastore --- START
   */
  int CID_POSITION = 5;

  int LOCAL_DATE_POSITION = 6;

  int VARIANT_CODE_POSITION = 8;

  int EXPERIMENT_CODE_POSITION = 9;

  int VERSION_NUMBER_POSITION = 10;

  int GUID_POSITION = 4;

  int TRANS_DATE_POSITION = 7;

  int NUM_TRANS_POSITION = 0;

  int BKG_GBV_POSITION = 1;

  int BKG_RN_POSITION = 2;

  int GROSS_PROFIT_POSITION = 3;

  /**
   * These field positions will be read from Hive Metastore --- END
   */

  int CID_SEGMENTATION_KEY = 1;

  String COL_DELIM = new String(new char[] { 1 });

  Pattern TAB_SEP_PATTERN = Pattern.compile(COL_DELIM);

  String SEGMENTATION_DATA_PATH_KEY = "segmentationDataPath";

  String REPORTING_DATA_PATH_KEY = "reportingDataPath";

  String HIVE_NULL_VALUE = "\\N";

}
