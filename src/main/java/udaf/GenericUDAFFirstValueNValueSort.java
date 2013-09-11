package udaf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import com.google.common.collect.Lists;

/**
 * This is a UDAF that returns the first column value appearing in the sorted
 * rows of a group. The rows are sorted by the column values provided along. Any
 * no. of sort column values can be provided.
 * 
 * Input values: 1. Column value to be returned 2. Variable no. of sort column
 * values
 * 
 * Output value: Returns column value for the 1st row in the sorted data rows.
 * 
 */
public class GenericUDAFFirstValueNValueSort extends
		AbstractGenericUDAFResolver {
	private static final Log LOG = LogFactory
			.getLog(GenericUDAFFirstValueNValueSort.class.getName());

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {
		if (parameters.length < 2) {
			throw new UDFArgumentTypeException(2,
					"Min 2 arguments expected: 1). colm value to be returned, "
							+ " 2. 1 or more orderBy colms");
		}
		// Only Primitive data types are allowed as sort columns
		for (int num = 1; num < parameters.length; num++) {
			if (parameters[num].getCategory() != ObjectInspector.Category.PRIMITIVE) {
				throw new UDFArgumentTypeException(num,
						"Only primitive type arguments are accepted in orderby colms but "
								+ parameters[num].getTypeName()
								+ " was passed as parameter " + num);
			}
			switch (((PrimitiveTypeInfo) parameters[num])
					.getPrimitiveCategory()) {
			case VOID:
			case UNKNOWN:
				throw new UDFArgumentTypeException(
						0,
						((PrimitiveTypeInfo) parameters[num])
								.getPrimitiveCategory()
								+ " arguments not acceptable in order by columns");
			default:
				break;
			}
		}
		return createEvaluator();
	}

	protected GenericUDAFEvaluator createEvaluator() {
		return new GenericUDAFOrderedNValueEvaluator();
	}

	/*
	 * Keeps track of the Top key-value pair in the group
	 */
	private static class MinOrderedSet implements AggregationBuffer {
		private Object[] sortKeys;
		private Object value;

		MinOrderedSet() {
			init();
		}

		void init() {
			sortKeys = null;
			value = null;
		}

		void checkAndSet(Object[] newkeys, ObjectInspector[] sortKeyOI,
				Object newValue) {
			if (sortKeys == null
					|| (newkeys != null && areNewSortKeysLesser(newkeys,
							sortKeyOI))) {
				sortKeys = newkeys;
				this.value = newValue;
			}
		}

		private boolean areNewSortKeysLesser(Object[] newKeys,
				ObjectInspector[] sortKeyOI) {
			int compareResult = ObjectInspectorUtils.compare(newKeys, sortKeyOI, sortKeys, sortKeyOI);
			if (compareResult < 0) {
				return true;
			} 
			return false;
		}

		Object getMinValue() {
			return value;
		}

		Map<Object, Object> getMinRecordAsMap() {
			Map<Object, Object> topRec = new HashMap<Object, Object>(1);
			topRec.put(value, sortKeys);
			return topRec;
		}
	}

	public static class GenericUDAFOrderedNValueEvaluator extends
			GenericUDAFEvaluator {
		private ObjectInspector valueOI;
		private ObjectInspector[] sortKeysOI;
		private ObjectInspector[] stdSortKeysOI;
		private StandardMapObjectInspector mapOI;
		private ObjectInspector writableValueOI;

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			// In complete & partial1 mode, we'll receive value to be returned &
			// sortKeys
			if (m.equals(Mode.COMPLETE) || m.equals(Mode.PARTIAL1)) {
				valueOI = parameters[0];
				sortKeysOI = new PrimitiveObjectInspector[parameters.length - 1];
				stdSortKeysOI = new ObjectInspector[parameters.length - 1];
				for (int num = 1; num < parameters.length; num++) {
					sortKeysOI[num - 1] = (PrimitiveObjectInspector) parameters[num];
					stdSortKeysOI[num - 1] = ObjectInspectorUtils
							.getStandardObjectInspector(parameters[num]);
				}
				writableValueOI = ObjectInspectorUtils
						.getWritableObjectInspector(valueOI);
			} else {
				// In partial2 & final mode,
				// we'll receive the intermediate map, with the top key-value
				// pair for that partial aggregation
				mapOI = (StandardMapObjectInspector) parameters[0];
				StructObjectInspector soi = (StructObjectInspector) mapOI
						.getMapValueObjectInspector();
				List<? extends StructField> sortKeysListStruct = soi
						.getAllStructFieldRefs();
				stdSortKeysOI = new ObjectInspector[sortKeysListStruct.size()];
				for (int i = 0; i < sortKeysListStruct.size(); i++) {
					stdSortKeysOI[i] = ObjectInspectorUtils
							.getStandardObjectInspector(sortKeysListStruct.get(
									i).getFieldObjectInspector());
				}
				writableValueOI = ObjectInspectorUtils
						.getWritableObjectInspector(mapOI
								.getMapKeyObjectInspector());
			}
			// In partial mode, the intermediate map containing top key-value
			// pair for the partial data is returned
			if (m == Mode.PARTIAL1) {
				ArrayList<String> keyNames = new ArrayList<String>();
				for (int num = 0; num < sortKeysOI.length; num++) {
					keyNames.add("k" + num);
				}
				ObjectInspector mapOI = ObjectInspectorFactory
						.getStandardMapObjectInspector(
								writableValueOI,
								ObjectInspectorFactory.getStandardStructObjectInspector(
										keyNames,
										Lists.newArrayList(stdSortKeysOI)));
				return ObjectInspectorUtils.getWritableObjectInspector(mapOI);
			} else if (m == Mode.PARTIAL2) {
				return ObjectInspectorUtils.getWritableObjectInspector(mapOI);
			} else if (m == Mode.FINAL) {
				// In final mode, the aggregated top column value for the group
				// is returned
				ObjectInspector writableValueOI = ObjectInspectorUtils
						.getWritableObjectInspector(mapOI
								.getMapKeyObjectInspector());
				return writableValueOI;
			}
			return writableValueOI;
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			return new MinOrderedSet();
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			((MinOrderedSet) agg).init();
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters)
				throws HiveException {
			MinOrderedSet existingRecord = (MinOrderedSet) agg;
			Object value = ObjectInspectorUtils.copyToStandardObject(
					parameters[0], valueOI, ObjectInspectorCopyOption.WRITABLE);
			Object[] stdSortKeys = new Object[parameters.length-1];
			for (int num = 0; num < sortKeysOI.length; num++) {
				if (parameters[num + 1] != null) {
					Object key = ObjectInspectorUtils.copyToStandardObject(
							parameters[num + 1], sortKeysOI[num],
							ObjectInspectorCopyOption.WRITABLE);
					stdSortKeys[num] = key;
				} else {
					stdSortKeys[num] = null;
				}
			}
			// if the new sortKey is less than the current sortKey, set the new
			// one
			existingRecord.checkAndSet(stdSortKeys,
					stdSortKeysOI, value);
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg)
				throws HiveException {
			Object map = ((MinOrderedSet) agg).getMinRecordAsMap();
			return map;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			if (partial != null) {
				MinOrderedSet myagg = (MinOrderedSet) agg;
				Map<Object, Object> partialData = ((LazyBinaryMap) partial)
						.getMap();
				for (Map.Entry<Object, Object> data : partialData.entrySet()) {
					LazyBinaryStruct soi = (LazyBinaryStruct) data.getValue();
					List<Object> sortKeys = soi.getFieldsAsList();
					Object[] stdSortKeys = new Object[sortKeys.size()];
					for (int i = 0; i < sortKeys.size(); i++) {
						stdSortKeys[i] = ObjectInspectorUtils
								.copyToStandardObject(sortKeys.get(i),
										stdSortKeysOI[i],
										ObjectInspectorCopyOption.WRITABLE);
					}
					Object newVal = ObjectInspectorUtils.copyToStandardObject(data.getKey(), writableValueOI);
					myagg.checkAndSet(stdSortKeys,
							stdSortKeysOI, newVal);
				}
			}
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			return ((MinOrderedSet) agg).getMinValue();
		}
	}
}