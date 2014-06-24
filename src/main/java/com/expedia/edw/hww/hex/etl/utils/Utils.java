package com.expedia.edw.hww.hex.etl.utils;

import static com.expedia.edw.hww.hex.etl.Constants.HIVE_NULL_VALUE;

public final class Utils {
  private Utils() {
  }

  public static String coalesce(String... strings) {
    String curr = null;
    for (String s : strings) {
      if (s != null && !HIVE_NULL_VALUE.equalsIgnoreCase(s)) {
        curr = s;
        break;
      }
    }
    return curr;
  }

  public static boolean containsArrayInt(int[] haystack, int needle) {
    for (int hay : haystack) {
      if (hay == needle) {
        return true;
      }
    }
    return false;
  }

}
