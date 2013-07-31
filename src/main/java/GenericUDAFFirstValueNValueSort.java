import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

public class GenericUDAFFirstValueNValueSort extends AbstractGenericUDAFResolver {
	private static final Log LOG = LogFactory.getLog(GenericUDAFFirstValueNValueSort.class
			.getName());

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {
		if (parameters.length < 2) {// value to be returned, orderBy value,
			throw new UDFArgumentTypeException(
					2,
					"Min 2 arguments expected: 1). colm value to be returned, "
							+ " 2. 1 or more orderBy colms");
		}
		for(int num=1; num< parameters.length; num++) {
			if (parameters[num].getCategory() != ObjectInspector.Category.PRIMITIVE) {
		      throw new UDFArgumentTypeException(num,
		          "Only primitive type arguments are accepted in orderby colms but "
		          + parameters[num].getTypeName() + " was passed as parameter " + num);
		    }
			switch (((PrimitiveTypeInfo) parameters[num]).getPrimitiveCategory()) {
				case VOID:
				case UNKNOWN: 
					throw new UDFArgumentTypeException(0,
							((PrimitiveTypeInfo) parameters[num]).getPrimitiveCategory() + " arguments not acceptable in order by columns");
				default:
					break;
			}
		}
		return createEvaluator();
	}

	protected GenericUDAFEvaluator createEvaluator() {
		return new GenericUDAFOrderedNValueEvaluator();
	}

	private static class TopOrderedSet implements AggregationBuffer {
		private Text sortKey;
		private Object value;
		
		TopOrderedSet() {
			init();
		}

		void init() {
			sortKey = null;
			value = null;
		}

		void checkAndSet(Text key, Object value) {
			if(sortKey == null  || (key!=null && sortKey.compareTo(key)>0)) {
				sortKey = key;
				this.value = value; 
			}
		}

		public Object getTopValue() {
			return value;
		}
		
		public Map<Text, Object> getTopRecordAsMap() {
			Map<Text, Object> topRec = new HashMap<Text, Object>(1);
			topRec.put(sortKey, value);
			return topRec;
		}
		
	}

	public static class GenericUDAFOrderedNValueEvaluator extends
			GenericUDAFEvaluator {
		private ObjectInspector valueOI;
		private ObjectInspector[] sortKeyOI;
		private StandardMapObjectInspector mapOI;
		private ObjectInspector writableValueOI;
		
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			if (m.equals(Mode.COMPLETE) || m.equals(Mode.PARTIAL1)) {
				valueOI = parameters[0];
				sortKeyOI = new PrimitiveObjectInspector[parameters.length-1];
				for(int num=1; num<parameters.length; num++) {
					sortKeyOI[num-1] = (PrimitiveObjectInspector) parameters[num];
				}
				writableValueOI = ObjectInspectorUtils
						.getWritableObjectInspector(valueOI);
			} else {
				mapOI = (StandardMapObjectInspector) parameters[0];
			}
			
			// init output object inspectors
			if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
				ObjectInspector mapOI = ObjectInspectorFactory.getStandardMapObjectInspector(
						PrimitiveObjectInspectorFactory.writableStringObjectInspector, writableValueOI);
				return ObjectInspectorUtils
						.getWritableObjectInspector(mapOI);
			} else if(m == Mode.FINAL) {
				ObjectInspector writableValueOI = ObjectInspectorUtils
						.getWritableObjectInspector(mapOI.getMapValueObjectInspector());
				return writableValueOI;
			}
			return writableValueOI; 
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			return new TopOrderedSet();
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			((TopOrderedSet) agg).init();
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters)
				throws HiveException {
			TopOrderedSet fb = (TopOrderedSet) agg;
			Object value = ObjectInspectorUtils.copyToStandardObject(parameters[0],
					valueOI, ObjectInspectorCopyOption.WRITABLE);
			StringBuilder sortKey = new StringBuilder();
			for(int num=0; num<sortKeyOI.length; num++) {
				if(parameters[num]!=null) {
					Object key = ObjectInspectorUtils.copyToStandardObject(parameters[num],
							sortKeyOI[num], ObjectInspectorCopyOption.WRITABLE);
					sortKey.append(key);
				}
			}
			fb.checkAndSet(new Text(sortKey.toString()), value);
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg)
				throws HiveException {
			Object map = ((TopOrderedSet) agg).getTopRecordAsMap();
			return map;
		}
		
		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			if (partial != null) {
				TopOrderedSet myagg = (TopOrderedSet) agg;
				Map<Object, Object> partialData = ((LazyBinaryMap) partial).getMap();
				for(Map.Entry<Object, Object> data: partialData.entrySet()) {
					myagg.checkAndSet((Text) data.getKey(), data.getValue());
				}
		     }
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			return ((TopOrderedSet) agg).getTopValue();
		}
	}
}