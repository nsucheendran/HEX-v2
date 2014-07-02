package com.expedia.edw.hww.hex.etl;

import static org.junit.Assert.assertFalse;

import org.junit.Test;

public class CFInputFormatTest {
  @Test
  public void testSplitable() {
    CFInputFormat inputFormat = new CFInputFormat();
    assertFalse(inputFormat.isSplitable(null, null));
  }
}
