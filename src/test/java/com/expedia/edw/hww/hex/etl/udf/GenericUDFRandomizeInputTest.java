package com.expedia.edw.hww.hex.etl.udf;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Lists;

public class GenericUDFRandomizeInputTest {
  private final ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  private final ObjectInspector booleanOI = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  private final ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;

  @Test
  public void testInitializeLessArgs() throws UDFArgumentException {
    ObjectInspector[] objects = new ObjectInspector[3];

    GenericUDFRandomizeInput randomizeInput = new GenericUDFRandomizeInput();
    try {
      randomizeInput.initialize(objects);
    } catch (UDFArgumentLengthException e) {
      assertThat(e.getMessage(),
          is("The function repeatInput(inputValue, seed, seedValSeparator, randomize, whitelistValues) "
              + "requires atleast 3 arguments."));
    }
  }

  @Test
  public void testInitializePositiveCase() throws UDFArgumentException {
    ObjectInspector[] objects = getObjectInspectorArray();

    GenericUDFRandomizeInput randomizeInput = new GenericUDFRandomizeInput();
    ObjectInspector oi = randomizeInput.initialize(objects);
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
  public void testInitializeInvalidFirstArgNull() {
    ObjectInspector[] objects = getObjectInspectorArray();

    objects[0] = null;
    GenericUDFRandomizeInput randomizeInput = new GenericUDFRandomizeInput();
    try {
      randomizeInput.initialize(objects);
    } catch (UDFArgumentException e) {
      assertThat(e.getMessage(), is("first argument must be a primitive data type"));
    } catch (Exception e) {
      fail("Unexpected Exception.");
    }
  }

  @Test
  public void testInitializeInvalidFirstArg() {
    ObjectInspector[] objects = getObjectInspectorArray();

    objects[0] = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
    GenericUDFRandomizeInput randomizeInput = new GenericUDFRandomizeInput();
    try {
      randomizeInput.initialize(objects);
    } catch (UDFArgumentException e) {
      assertThat(e.getMessage(), is("first argument must be a primitive data type"));
    } catch (Exception e) {
      fail("Unexpected Exception.");
    }
  }

  @Test
  public void testInitializeInvalidSecondArgNull() {
    ObjectInspector[] objects = getObjectInspectorArray();

    objects[1] = null;
    GenericUDFRandomizeInput randomizeInput = new GenericUDFRandomizeInput();
    try {
      randomizeInput.initialize(objects);
    } catch (UDFArgumentException e) {
      assertThat(e.getMessage(), is("second argument must be of type int"));
    } catch (Exception e) {
      fail("Unexpected Exception.");
    }
  }

  @Test
  public void testInitializeInvalidThirdArgNull() {
    ObjectInspector[] objects = getObjectInspectorArray();

    objects[2] = null;
    GenericUDFRandomizeInput randomizeInput = new GenericUDFRandomizeInput();
    try {
      randomizeInput.initialize(objects);
    } catch (UDFArgumentException e) {
      assertThat(e.getMessage(), is("third argument must be of type String"));
    } catch (Exception e) {
      fail("Unexpected Exception.");
    }
  }

  @Test
  public void testInitializeInvalidForthArg() {
    ObjectInspector[] objects = getObjectInspectorArray();

    objects[3] = stringOI;
    GenericUDFRandomizeInput randomizeInput = new GenericUDFRandomizeInput();
    try {
      randomizeInput.initialize(objects);
    } catch (UDFArgumentException e) {
      assertThat(e.getMessage(), is("fourth argument must be of type Boolean"));
    } catch (Exception e) {
      fail("Unexpected Exception.");
    }
  }

  @Test
  public void testInitializeInvalidFifthArgNull() {
    ObjectInspector[] objects = getObjectInspectorArray();

    objects[4] = null;
    GenericUDFRandomizeInput randomizeInput = new GenericUDFRandomizeInput();
    try {
      randomizeInput.initialize(objects);
    } catch (UDFArgumentException e) {
      fail("Unexpected Exception.");
    }
  }

  @Test
  public void testInitializeInvalidFifthArgInvalid() {
    ObjectInspector[] objects = getObjectInspectorArray();

    objects[4] = booleanOI;
    GenericUDFRandomizeInput randomizeInput = new GenericUDFRandomizeInput();
    try {
      randomizeInput.initialize(objects);
    } catch (UDFArgumentException e) {
      assertThat(e.getMessage(), is("fifth argument must be a list / array"));
    } catch (Exception e) {
      fail("Unexpected Exception.");
    }
  }

  @Test
  public void testEvaluateNullInput() throws Exception {
    DeferredObject[] arguments = new DeferredObject[5];
    arguments[0] = new GenericUDF.DeferredJavaObject(null);
    arguments[1] = new GenericUDF.DeferredJavaObject(null);
    arguments[2] = new GenericUDF.DeferredJavaObject(null);
    arguments[3] = new GenericUDF.DeferredJavaObject(null);
    arguments[4] = new GenericUDF.DeferredJavaObject(null);

    GenericUDFRandomizeInput randomizeInput = getGenericUDFRandomizeInput();

    Object o = randomizeInput.evaluate(arguments);
    assertNull(o);
  }

  @Test
  public void testEvaluateGenerateDimensionData() throws Exception {
    testEvaluateData(false, false);
  }

  @Test
  public void testEvaluateInputDataSplit() throws Exception {
    testEvaluateData(true, false);
  }

  @Test
  public void testEvaluateNewText() throws Exception {
    testEvaluateData(true, true);
  }

  @Test
  public void testGetDisplayString() {
    String[] children = new String[5];
    children[0] = "one";
    children[1] = "two";
    children[2] = "three";
    children[3] = "four";
    children[4] = "five";
    GenericUDFRandomizeInput randomizeInput = new GenericUDFRandomizeInput();
    String displayString = randomizeInput.getDisplayString(children);
    assertThat(displayString, is("randomize(one, two, three, four, five)"));
  }

  private GenericUDFRandomizeInput getGenericUDFRandomizeInput() throws Exception {
    ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ListObjectInspector loi = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);

    GenericUDFRandomizeInput randomizeInput = new GenericUDFRandomizeInput();

    Field listOIField = GenericUDFRandomizeInput.class.getDeclaredField("whitelistValuesOI");
    listOIField.setAccessible(true);
    listOIField.set(randomizeInput, loi);

    return randomizeInput;
  }

  private ObjectInspector[] getObjectInspectorArray() {
    ObjectInspector[] objects = new ObjectInspector[5];

    objects[0] = stringOI;
    objects[1] = intOI;
    objects[2] = stringOI;
    objects[3] = booleanOI;
    objects[4] = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);

    return objects;
  }

  private void testEvaluateData(boolean isRandom, boolean isNewInput) throws Exception {
    String inputStr = "Test";
    int inputSeedSize = 5;
    String inputSeedValSeparator = "###";
    Text inputText = new Text(inputStr);
    List<Text> inputList;
    if (isNewInput) {
      inputList = Lists.newArrayList();
    } else {
      inputList = Lists.newArrayList(inputText);
    }
    DeferredObject[] arguments = new DeferredObject[5];
    arguments[0] = new GenericUDF.DeferredJavaObject(inputText);
    IntWritable intWritable = new IntWritable(inputSeedSize);
    arguments[1] = new GenericUDF.DeferredJavaObject(intWritable);
    arguments[2] = new GenericUDF.DeferredJavaObject(inputSeedValSeparator);
    BooleanWritable booleanWritable = new BooleanWritable(isRandom);
    arguments[3] = new GenericUDF.DeferredJavaObject(booleanWritable);
    arguments[4] = new GenericUDF.DeferredJavaObject(inputList);

    GenericUDFRandomizeInput randomizeInput = getGenericUDFRandomizeInput();

    Object o = randomizeInput.evaluate(arguments);
    if (o instanceof List<?>) {
      List<Text> list = (List<Text>) o;
      if (isNewInput) {
        assertThat(list.size(), is(1));
        assertTrue(list.contains(inputText));
      } else {
        if (isRandom) {
          assertThat(list.size(), is(1));
          // Input will be generated as Test### with trailing random number between 0 - 4.
          Pattern p = Pattern.compile(inputStr + inputSeedValSeparator + "[0-4]");
          Matcher m = p.matcher(list.get(0).toString());
          assertTrue(m.matches());
        } else {
          assertThat(list.size(), is(inputSeedSize));
          for (int i = 0; i < inputSeedSize; i++) {
            assertTrue(list.contains(new Text(inputStr + inputSeedValSeparator + i)));
          }
        }
      }
    } else {
      fail("Unexpected return type from UDF");
    }
  }
}
