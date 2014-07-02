package com.expedia.edw.hww.hex.etl.udaf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.junit.Test;

import com.expedia.edw.hww.hex.etl.udaf.GenericUDAFFirstValueNValueSort.GenericUDAFOrderedNValueEvaluator;
import com.expedia.edw.hww.hex.etl.udaf.GenericUDAFFirstValueNValueSort.MinOrderedSet;
public class GenericUDAFFirstValueNValueSortTest {

  @Test(expected = UDFArgumentTypeException.class)
  public void testGetEvaluatorLessParams() throws SemanticException {
    TypeInfo[] typeInfos = new TypeInfo[0];
    GenericUDAFFirstValueNValueSort firstValueNValueSort = new GenericUDAFFirstValueNValueSort();

    firstValueNValueSort.getEvaluator(typeInfos);
  }

  @Test
  public void testCreateEvaluator() {
    GenericUDAFFirstValueNValueSort firstValueNValueSort = new GenericUDAFFirstValueNValueSort();

    GenericUDAFEvaluator evaluator = firstValueNValueSort.createEvaluator();

    if (!(evaluator instanceof GenericUDAFOrderedNValueEvaluator)) {
      fail("Unexpected returned type");
    }
  }

  @Test(expected = UDFArgumentTypeException.class)
  public void testGetEvaluatorInvalidArgs() throws SemanticException {
    TypeInfo[] typeInfos = new TypeInfo[2];
    typeInfos[0] = new PrimitiveTypeInfo();

    // Passing 2nd argument as non-primitive type
    typeInfos[1] = new TypeInfo() {

      private static final long serialVersionUID = 1L;

      @Override
      public int hashCode() {
        return 0;
      }

      @Override
      public String getTypeName() {
        return null;
      }

      @Override
      public Category getCategory() {
        return Category.UNION;
      }

      @Override
      public boolean equals(Object arg0) {
        return false;
      }
    };

    GenericUDAFFirstValueNValueSort firstValueNValueSort = new GenericUDAFFirstValueNValueSort();

    firstValueNValueSort.getEvaluator(typeInfos);
  }

  @Test
  public void testMinOrderedSetNullInput() {
    MinOrderedSet minOrderedSet = new MinOrderedSet();

    minOrderedSet.checkAndSet(null, null, null);

    Map<Object, Object> map = minOrderedSet.getMinRecordAsMap();
    assertThat(map.size(), is(1));
    assertNull(map.keySet().iterator().next());
    assertNull(minOrderedSet.getMinValue());
    assertNull(map.get(map.keySet().iterator().next()));
  }

  @Test
  public void testMinOrderedSetSingleInput() {
    String key = "newKey";
    String value = "value";

    MinOrderedSet minOrderedSet = new MinOrderedSet();
    Object[] keys = new Object[1];
    keys[0] = key;
    minOrderedSet.checkAndSet(keys, null, value);

    Map<Object, Object> map = minOrderedSet.getMinRecordAsMap();
    assertThat(map.size(), is(1));
    assertThat(minOrderedSet.getMinValue().toString(), is(value));
    assertTrue(map.containsKey(value));
    assertArrayEquals((Object []) map.get(value), keys);
  }

  @Test
  public void testMinOrderedSetMultipleValues() {
    String keyOld = "oldKey";
    String valueOld = "oldValue";

    String keyNew = "newKey";
    String valueNew = "newValue";

    ObjectInspector[] oiArray = new ObjectInspector[1];
    oiArray[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    MinOrderedSet minOrderedSet = new MinOrderedSet();
    Object[] keys = new Object[1];
    keys[0] = keyOld;
    minOrderedSet.checkAndSet(keys, oiArray, valueOld);

    keys = new Object[1];
    keys[0] = keyNew;
    minOrderedSet.checkAndSet(keys, oiArray, valueNew);

    Map<Object, Object> map = minOrderedSet.getMinRecordAsMap();
    assertThat(map.size(), is(1));
    assertThat(minOrderedSet.getMinValue().toString(), is(valueNew));
    assertTrue(map.containsKey(valueNew));
    assertArrayEquals((Object []) map.get(valueNew), keys);
  }

  @Test
  public void testMinOrderedSetMultipleValuesReverseOrder() {
    String keyOld = "newKey";
    String valueOld = "newValue";

    String keyNew = "oldKey";
    String valueNew = "OldValue";

    ObjectInspector[] oiArray = new ObjectInspector[1];
    oiArray[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    MinOrderedSet minOrderedSet = new MinOrderedSet();
    Object[] keysOld = new Object[1];
    keysOld[0] = keyOld;
    minOrderedSet.checkAndSet(keysOld, oiArray, valueOld);

    Object[] keysNew = new Object[1];
    keysNew[0] = keyNew;
    minOrderedSet.checkAndSet(keysNew, oiArray, valueNew);

    Map<Object, Object> map = minOrderedSet.getMinRecordAsMap();
    assertThat(map.size(), is(1));
    assertThat(minOrderedSet.getMinValue().toString(), is(valueOld));
    assertTrue(map.containsKey(valueOld));
    assertArrayEquals((Object []) map.get(valueOld), keysOld);
  }


  @Test
  public void testGenericUDAFOrderedNValueEvaluatorAggregationBuffer() throws HiveException {
    GenericUDAFOrderedNValueEvaluator evaluator = new GenericUDAFOrderedNValueEvaluator();
    AggregationBuffer aggBuffer = evaluator.getNewAggregationBuffer();

    assertThat(aggBuffer.getClass().getName(), is(MinOrderedSet.class.getName()));

    MinOrderedSet minOrderedSet = (MinOrderedSet) aggBuffer;
    String key = "Key";
    String value = "Value";

    Object[] keys = new Object[1];
    keys[0] = key;
    minOrderedSet.checkAndSet(keys, null, value);

    Map<Object, Object> map = minOrderedSet.getMinRecordAsMap();
    assertThat(map.size(), is(1));
    assertThat(minOrderedSet.getMinValue().toString(), is(value));
    assertTrue(map.containsKey(value));
    assertArrayEquals((Object []) map.get(value), keys);

    Object o = evaluator.terminatePartial(aggBuffer);
    if (o instanceof Map<?, ?>) {
      assertEquals(map, o);
    } else {
      fail ("Returned value is different. Expected is Map.");
    }

    o = evaluator.terminate(aggBuffer);
    if (o instanceof String) {
      assertEquals(value, o);
    } else {
      fail ("Returned value is different. Expected is String.");
    }

    evaluator.reset(aggBuffer);

    map = minOrderedSet.getMinRecordAsMap();
    assertThat(map.size(), is(1));
    assertNull(map.keySet().iterator().next());
    assertNull(minOrderedSet.getMinValue());
    assertNull(map.get(map.keySet().iterator().next()));
  }

  @Test
  public void testGenericUDAFOrderedNValueEvaluatorInitCompleteMode() throws HiveException {
    testCompleteAndPartial1(Mode.COMPLETE, WritableStringObjectInspector.class);
  }

  @Test
  public void testGenericUDAFOrderedNValueEvaluatorInitPartial1Mode() throws HiveException {
    testCompleteAndPartial1(Mode.PARTIAL1, StandardMapObjectInspector.class);
  }

  @Test
  public void testGenericUDAFOrderedNValueEvaluatorInitPartial2Mode() throws HiveException {
    testFinalAndPartial2(Mode.PARTIAL2, StandardConstantMapObjectInspector.class);
  }

  @Test
  public void testGenericUDAFOrderedNValueEvaluatorInitFinalMode() throws HiveException {
    testFinalAndPartial2(Mode.FINAL, WritableStringObjectInspector.class);
  }

  private void testFinalAndPartial2(Mode m, Class<?> c) throws HiveException {
    ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    List<String> structFieldNames = new ArrayList<String>();
    structFieldNames.add("field1");
    List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
    structFieldObjectInspectors.add(stringOI);
    ObjectInspector structOI = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
    GenericUDAFOrderedNValueEvaluator evaluator = new GenericUDAFOrderedNValueEvaluator();
    ObjectInspector[] parameters = new ObjectInspector[2];
    parameters[0] = ObjectInspectorFactory.getStandardConstantMapObjectInspector(stringOI, structOI, null);
    parameters[1] = structOI;
    ObjectInspector oi = evaluator.init(m, parameters);

    assertThat(oi.getClass().getName(), is(c.getName()));
  }

  private void testCompleteAndPartial1(Mode m, Class<?> c) throws HiveException {
    GenericUDAFOrderedNValueEvaluator evaluator = new GenericUDAFOrderedNValueEvaluator();
    ObjectInspector[] parameters = new ObjectInspector[2];
    parameters[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    parameters[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector oi = evaluator.init(m, parameters);

    assertThat(oi.getClass().getName(), is(c.getName()));
  }
}
