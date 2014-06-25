package com.expedia.edw.hww.hex.etl.udf;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class GenericUDFApplyPatternOnListTest {

  @Test
  public void testInitializePositiveCase() throws UDFArgumentException {
    ObjectInspector[] objects = getObjectInspectorArray();

    GenericUDFApplyPatternOnList patternOnList = new GenericUDFApplyPatternOnList();
    ObjectInspector oi = patternOnList.initialize(objects);
    if (oi instanceof ListObjectInspector) {
      ListObjectInspector loi = (ListObjectInspector) oi;
      if (!(loi.getListElementObjectInspector() instanceof StringObjectInspector)) {
        fail("Return Type is not the list of StringObjectInspector");
      }
    } else {
      fail("Return Type is not the list");
    }
  }

  @Test
  public void testInitializeLessArgs() throws UDFArgumentException {
    testInitializeArgs(3);
  }

  @Test
  public void testInitializeMoreArgs() throws UDFArgumentException {
    testInitializeArgs(5);
  }

  @Test
  public void testInitializeInvalidFirstArg() {
    ObjectInspector[] objects = getObjectInspectorArray();

    ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    objects[0] = stringOI;
    GenericUDFApplyPatternOnList patternOnList = new GenericUDFApplyPatternOnList();
    try {
      patternOnList.initialize(objects);
    } catch (UDFArgumentException e) {
      assertThat(e.getMessage(), is("first argument must be a list / array"));
    } catch (Exception e) {
      fail("Unexpected Exception.");
    }
  }

  @Test
  public void testInitializeInvalidSecondArg() {
    ObjectInspector[] objects = getObjectInspectorArray();

    objects[1] = null;
    GenericUDFApplyPatternOnList patternOnList = new GenericUDFApplyPatternOnList();
    try {
      patternOnList.initialize(objects);
    } catch (UDFArgumentException e) {
      assertThat(e.getMessage(), is("second argument must be of type string"));
    } catch (Exception e) {
      fail("Unexpected Exception.");
    }
  }

  @Test
  public void testInitializeInvalidThirdArg() {
    ObjectInspector[] objects = getObjectInspectorArray();

    objects[2] = null;
    GenericUDFApplyPatternOnList patternOnList = new GenericUDFApplyPatternOnList();
    try {
      patternOnList.initialize(objects);
    } catch (UDFArgumentException e) {
      assertThat(e.getMessage(), is("third argument must be of type string"));
    } catch (Exception e) {
      fail("Unexpected Exception.");
    }
  }

  @Test
  public void testInitializeInvalidForthArg() {
    ObjectInspector[] objects = getObjectInspectorArray();

    objects[3] = null;
    GenericUDFApplyPatternOnList patternOnList = new GenericUDFApplyPatternOnList();
    try {
      patternOnList.initialize(objects);
    } catch (UDFArgumentException e) {
      assertThat(e.getMessage(), is("fourth argument must be of type boolean"));
    } catch (Exception e) {
      fail("Unexpected Exception.");
    }
  }

  @Test
  public void testInitializeInvalidListType() {
    ObjectInspector[] objects = getObjectInspectorArray();

    ObjectInspector booleanOI = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    objects[0] = ObjectInspectorFactory.getStandardListObjectInspector(booleanOI);

    GenericUDFApplyPatternOnList patternOnList = new GenericUDFApplyPatternOnList();
    try {
      patternOnList.initialize(objects);
    } catch (UDFArgumentException e) {
      assertThat(e.getMessage(), is("first argument must be a list of type string"));
    } catch (Exception e) {
      fail("Unexpected Exception.");
    }
  }

  @Test
  public void testEvaluateNullInput() throws Exception {
    DeferredObject[] arguments = new DeferredObject[4];
    arguments[0] = new GenericUDF.DeferredJavaObject(null);
    arguments[1] = new GenericUDF.DeferredJavaObject(null);
    arguments[2] = new GenericUDF.DeferredJavaObject(null);
    arguments[3] = new GenericUDF.DeferredJavaObject(null);

    GenericUDFApplyPatternOnList patternOnList = getGenericUDFApplyPatternOnList();

    Object o = patternOnList.evaluate(arguments);
    assertNull(o);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testEvaluateEmptyList() throws Exception {
    DeferredObject[] arguments = new DeferredObject[4];
    arguments[0] = new GenericUDF.DeferredJavaObject(new ArrayList<Text>());
    arguments[1] = new GenericUDF.DeferredJavaObject("([^\\.]*)([\\.])(.*)");
    arguments[2] = new GenericUDF.DeferredJavaObject("$1$2%");
    BooleanWritable booleanWritable = new BooleanWritable(true);
    arguments[3] = new GenericUDF.DeferredJavaObject(booleanWritable);

    GenericUDFApplyPatternOnList patternOnList = getGenericUDFApplyPatternOnList();

    Object o = patternOnList.evaluate(arguments);
    if (o instanceof List<?>) {
      List<Text> list = (List<Text>) o;
      assertTrue(list.isEmpty());
    } else {
      fail("Unexpected return type from UDF");
    }
  }

  @Test
  public void testEvaluateWithAppend() throws Exception {
    testEvaluateAppend(true);
  }

  @Test
  public void testEvaluateWithoutAppend() throws Exception {
    testEvaluateAppend(false);
  }

  @Test
  public void testGetDisplayString() {
    String[] children = new String[4];
    children[0] = "one";
    children[1] = "two";
    children[2] = "three";
    children[3] = "four";
    GenericUDFApplyPatternOnList patternOnList = new GenericUDFApplyPatternOnList();
    String displayString = patternOnList.getDisplayString(children);
    assertThat(displayString, is("applyPattern(one, two, three, four)"));
  }

  private void testInitializeArgs(int size) throws UDFArgumentException {
    ObjectInspector[] objects = new ObjectInspector[size];

    GenericUDFApplyPatternOnList patternOnList = new GenericUDFApplyPatternOnList();
    try {
      patternOnList.initialize(objects);
    } catch (UDFArgumentLengthException e) {
      assertThat(e.getMessage(), is("The function applyPattern(array, regexPattern, replacePattern, "
          + "includeSourceArrayInOutput) takes exactly 4 arguments."));
    }
  }

  private GenericUDFApplyPatternOnList getGenericUDFApplyPatternOnList() throws Exception {
    ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ListObjectInspector loi = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);

    GenericUDFApplyPatternOnList patternOnList = new GenericUDFApplyPatternOnList();

    Field listOIField = GenericUDFApplyPatternOnList.class.getDeclaredField("listOI");
    listOIField.setAccessible(true);
    listOIField.set(patternOnList, loi);

    return patternOnList;
  }

  private ObjectInspector[] getObjectInspectorArray() {
    ObjectInspector[] objects = new ObjectInspector[4];

    ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector booleanOI = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;

    objects[0] = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
    objects[1] = stringOI;
    objects[2] = stringOI;
    objects[3] = booleanOI;

    return objects;
  }

  @SuppressWarnings("unchecked")
  private void testEvaluateAppend(boolean isinputAppended) throws Exception {
    List<Text> inputTexts = new ArrayList<Text>();
    inputTexts.add(new Text("H001.001"));
    inputTexts.add(new Text("H001.002"));
    inputTexts.add(new Text("H001.003"));
    inputTexts.add(new Text("H002.001"));
    inputTexts.add(new Text("H002.002"));

    Set<Text> expectedTexts = new HashSet<Text>();
    if (isinputAppended) {
      expectedTexts.addAll(inputTexts);
    }
    expectedTexts.add(new Text("H001.%"));
    expectedTexts.add(new Text("H002.%"));
    DeferredObject[] arguments = new DeferredObject[4];
    arguments[0] = new GenericUDF.DeferredJavaObject(inputTexts);
    arguments[1] = new GenericUDF.DeferredJavaObject("([^\\.]*)([\\.])(.*)");
    arguments[2] = new GenericUDF.DeferredJavaObject("$1$2%");
    BooleanWritable booleanWritable = new BooleanWritable(isinputAppended);
    arguments[3] = new GenericUDF.DeferredJavaObject(booleanWritable);

    GenericUDFApplyPatternOnList patternOnList = getGenericUDFApplyPatternOnList();

    Object o = patternOnList.evaluate(arguments);
    if (o instanceof List<?>) {
      List<Text> actualTextList = (List<Text>) o;
      Set<Text> actualTextSet = new HashSet<Text>();
      actualTextSet.addAll(actualTextList);
      assertThat(actualTextSet, equalTo(expectedTexts));
    } else {
      fail("Unexpected return type from UDF");
    }
  }
}
