package udf;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * GenericUDFPattern. 1. Array to be worked on 2. Pattern to be applied to each array element 3. Replace pattern 4.
 * Boolean indicating Include/Exclude array elements
 */
public class GenericUDFApplyPatternOnList extends GenericUDF {
  private ListObjectInspector listOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 4) {
      throw new UDFArgumentLengthException(
          "The function applyPattern(array, regexPattern, replacePattern, includeSourceArrayInOutput) "
              + "takes exactly 4 arguments.");
    }
    // 1. Check we received the right object types.
    ObjectInspector inputArrOI = ObjectInspectorUtils.getWritableObjectInspector(arguments[0]);
    ObjectInspector patternOI = arguments[1];
    ObjectInspector regexOI = arguments[2];
    ObjectInspector includeSourceArrayInOutputOI = arguments[3];
    if (!(inputArrOI instanceof ListObjectInspector)) {
      throw new UDFArgumentException("first argument must be a list / array");
    }
    if (!(patternOI instanceof StringObjectInspector)) {
      throw new UDFArgumentException("second argument must be of type string");
    }
    if (!(regexOI instanceof StringObjectInspector)) {
      throw new UDFArgumentException("third argument must be of type string");
    }
    if (!(includeSourceArrayInOutputOI instanceof BooleanObjectInspector)) {
      throw new UDFArgumentException("fourth argument must be of type boolean");
    }
    this.listOI = (ListObjectInspector) inputArrOI;
    if (!(listOI.getListElementObjectInspector() instanceof StringObjectInspector)) {
      throw new UDFArgumentException("first argument must be a list of type string");
    }
    return ObjectInspectorFactory
        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == 4);
    List<Text> list = (List<Text>) this.listOI.getList(arguments[0].get());
    String pattern = (String) ObjectInspectorUtils.copyToStandardJavaObject(arguments[1].get(),
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    String replaceRegex = (String) ObjectInspectorUtils.copyToStandardJavaObject(arguments[2].get(),
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    Boolean includeSourceArrayInOutput = (Boolean) ObjectInspectorUtils.copyToStandardJavaObject(arguments[3].get(),
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);

    // check for nulls
    if (list == null || pattern == null || replaceRegex == null) {
      return null;
    }
    if (includeSourceArrayInOutput == null) {
      includeSourceArrayInOutput = true;
    }
    Set<Text> patternMatchSet = Sets.newHashSet();
    for (Text s : list) {
      String str = s.toString();
      if (str.matches(pattern)) {
        patternMatchSet.add(new Text(str.replaceAll(pattern, replaceRegex)));
      }
    }
    List<Text> patternMatchList = Lists.newArrayList(patternMatchSet);
    if (includeSourceArrayInOutput) {
      patternMatchList.addAll(list);
    }
    return patternMatchList;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 4);
    return "applyPattern(" + children[0] + ", " + children[1] + ", " + children[2] + ", " + children[3] + ")";
  }

}
