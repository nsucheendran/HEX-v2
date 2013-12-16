package mr.utils;

import static mr.Constants.HIVE_NULL_VALUE;

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

}

