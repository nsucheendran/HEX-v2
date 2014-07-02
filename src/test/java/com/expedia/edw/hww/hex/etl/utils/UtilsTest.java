package com.expedia.edw.hww.hex.etl.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.expedia.edw.hww.hex.etl.Constants;

public class UtilsTest {
  @Test
  public void testCoalesceNullInput() {
    String result = Utils.coalesce();
    assertNull(result);
  }

  @Test
  public void testCoalesceSingleInput() {
    String input = "First";
    String result = Utils.coalesce(input);
    assertThat(result, is(input));
  }

  @Test
  public void testCoalesceMultipleInput() {
    String input1 = "First";
    String input2 = "Second";
    String input3 = "Third";

    String result = Utils.coalesce(null, Constants.HIVE_NULL_VALUE, input1, null, input2, null, input3);
    assertThat(result, is(input1));
  }

  @Test
  public void testContainsArrayIntEmptyHayStack() {
    assertFalse(Utils.containsArrayInt(new int[0], 1));
  }

  @Test
  public void testContainsArrayIntDisjointValues() {
    assertFalse(Utils.containsArrayInt(new int[] { 0, 2, 3, 4 }, 1));
  }

  @Test
  public void testContainsArrayIntValidValues() {
    assertTrue(Utils.containsArrayInt(new int[] { 0, 1, 2, 3, 4 }, 1));
  }
}
