package udf;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

// Return output as List of STRINGS (always), independent of the input dataType
public class GenericUDFRandomizeInputV2 extends GenericUDF {
  private ListObjectInspector whitelistValuesOI;
  private Map<Text, Integer> circularValueAssignmentTracker;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 4) {
      throw new UDFArgumentLengthException("The function repeatInput(inputValue, seed, seedValSeparator, randomize, whitelistValues) "
          + "requires atleast 3 arguments.");
    }
    circularValueAssignmentTracker = Maps.newHashMap();
    // 1. Check we received the right object types.
    ObjectInspector inputValOI = arguments[0];
    ObjectInspector seedOI = arguments[1];
    ObjectInspector seedValSeparatorOI = arguments[2];
    ObjectInspector randomizeOI = null;
    if (arguments.length >= 4) {
      randomizeOI = arguments[3];
    }
    if (arguments.length == 5) {
      if (arguments[4] != null && !(arguments[4] instanceof ListObjectInspector)) {
        throw new UDFArgumentException("fifth argument must be a list / array");
      }
      this.whitelistValuesOI = (ListObjectInspector)arguments[4];
    }
    if (!(inputValOI instanceof PrimitiveObjectInspector)) {
      throw new UDFArgumentException("first argument must be a primitive data type");
    }
    if (!(seedOI instanceof IntObjectInspector)) {
      throw new UDFArgumentException("second argument must be of type int");
    }
    if (!(seedValSeparatorOI instanceof StringObjectInspector)) {
      throw new UDFArgumentException("third argument must be of type String");
    }
    if (randomizeOI != null && !(randomizeOI instanceof BooleanObjectInspector)) {
      throw new UDFArgumentException("fourth argument must be of type Boolean");
    }
    return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String inputVal = (String) ObjectInspectorUtils.copyToStandardJavaObject(arguments[0].get(),
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    Integer seed = (Integer) ObjectInspectorUtils.copyToStandardJavaObject(arguments[1].get(),
        PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    String seedValSeparator = (String) ObjectInspectorUtils.copyToStandardJavaObject(arguments[2].get(),
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    Boolean randomizeInput = null;
    if (arguments.length >= 4) {
      randomizeInput = (Boolean) ObjectInspectorUtils.copyToStandardJavaObject(arguments[3].get(),
          PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
    }
    List<String> whitelist = null;
    if (whitelistValuesOI != null) {
      whitelist = (List<String>) this.whitelistValuesOI.getList(arguments[4].get());
    }
    // check for nulls
    if (inputVal == null || seed == null || seedValSeparator == null) {
      return null;
    }
    Text inputValText = new Text(inputVal);
    if (whitelist != null && !whitelist.contains(inputValText)) {
      return Lists.newArrayList(inputValText);
    }
    if (randomizeInput == null || randomizeInput) {
      Integer inc = circularValueAssignmentTracker.get(inputValText);
      if (inc == null || (inc + 1) == seed) {
        inc = -1;
      }
      circularValueAssignmentTracker.put(inputValText, ++inc);
      return Lists.newArrayList(new Text(inputValText + seedValSeparator + inc));
    }
    List<Text> repInputValues = Lists.newArrayList();
    for (int num = 0; num < seed; num++) {
      repInputValues.add(new Text(inputVal + seedValSeparator + num));
    }
    return repInputValues;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 5);
    return "randomize(" + children[0] + ", " + children[1] + ", " + children[2] + ", " + children[3] + ", " + children[4] + ")";
  }
}
