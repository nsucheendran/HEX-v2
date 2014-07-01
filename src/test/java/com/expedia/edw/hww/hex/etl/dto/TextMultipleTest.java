package com.expedia.edw.hww.hex.etl.dto;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TextMultipleTest {
  @Test
  public void testEqualsSame() {
    TextMultiple textMultiple = new TextMultiple();

    assertTrue(textMultiple.equals(textMultiple));
  }

  @Test
  public void testEqualsNull() {
    TextMultiple textMultiple = new TextMultiple();

    assertFalse(textMultiple.equals(null));
  }

  @Test
  public void testCompareToSame() {
    TextMultiple textMultiple = new TextMultiple();

    assertThat(textMultiple.compareTo(textMultiple), is(0));
  }

  @Test
  public void testCompareToDifferentOne() {
    TextMultiple textMultipleOne = new TextMultiple("Str1");
    TextMultiple textMultipleTwo = new TextMultiple("Str2");

    assertThat(textMultipleOne.compareTo(textMultipleTwo), is(-1));
  }

  @Test
  public void testCompareToDifferentTwo() {
    TextMultiple textMultipleOne = new TextMultiple("Str1");
    TextMultiple textMultipleTwo = new TextMultiple("Str2");

    assertThat(textMultipleTwo.compareTo(textMultipleOne), is(1));
  }

  @Test
  public void testCompareToDifferentThree() {
    TextMultiple textMultipleOne = new TextMultiple("Str1");
    TextMultiple textMultipleTwo = new TextMultiple("Str2", "Str3");

    assertThat(textMultipleOne.compareTo(textMultipleTwo), is(-1));
  }

  @Test
  public void testCompareToDifferentFour() {
    TextMultiple textMultipleOne = new TextMultiple("Str1");
    TextMultiple textMultipleTwo = new TextMultiple("Str2", "Str3");

    assertThat(textMultipleTwo.compareTo(textMultipleOne), is(1));
  }

  @Test
  public void testHashCode() {
    TextMultiple textMultipleOne = new TextMultiple();
    assertThat(textMultipleOne.hashCode(), is(1));

    TextMultiple textMultipleTwo = new TextMultiple("Str1");
    assertThat(textMultipleTwo.hashCode(), is(3511264));
  }
}
