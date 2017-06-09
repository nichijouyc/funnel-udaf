/*
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.hive.udf.funnel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@UDFType(deterministic = true)
@Description(name = "preprocess_merge_funnel", value = "_FUNC_(funnel_column) - Merges funnels. Use with funnel UDF.", extended = "Example: SELECT preprocess_merge_funnel(funnel)\n"
        + "         FROM (SELECT funnel(action, timestamp, array('signup_page', 'email_signup'), \n"
        + "                                                array('confirm_button'),\n"
        + "                                                array('submit_button')) AS funnel\n"
        + "               FROM table\n" + "               GROUP BY user_id) t;")
public class PreprocessFunnelMerge extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(PreprocessFunnelMerge.class.getName());

    @Override
    public MergeEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        // Get the parameters
        TypeInfo[] parameters = info.getParameters();

        // Check number of arguments
        if (parameters.length != 2) {
            throw new UDFArgumentLengthException("Please specify the funnel column.");
        }

        // Check if the id parameter is PRIMITIVE
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only PRIMITIVE type arguments are accepted but "
                    + parameters[0].getTypeName() + " was passed as the first parameter.");
        }

        // Check if the funnel parameter is not a list
        if (parameters[1].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(1, "Only list type arguments are accepted but "
                    + parameters[1].getTypeName() + " was passed as the second parameter.");
        }

        // Check that the list is an array of primitives
        if (((ListTypeInfo) parameters[1]).getListElementTypeInfo()
                .getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(1, "A long array argument should be passed, but "
                    + parameters[1].getTypeName() + " was passed instead.");
        }

        // Check that the list is of type long
        // May want to add support for int/double/float later
        switch (((PrimitiveTypeInfo) ((ListTypeInfo) parameters[1]).getListElementTypeInfo()).getPrimitiveCategory()) {
            case LONG :
                break;
            default :
                throw new UDFArgumentTypeException(1, "A long array argument should be passed, but "
                        + parameters[1].getTypeName() + " was passed instead.");
        }

        return new MergeEvaluator();
    }

    public static class MergeEvaluator extends GenericUDAFEvaluator {
        /** Input list object inspector. Used during iterate. */
        private ObjectInspector idObjectInspector;
        private ListObjectInspector listObjectInspector;
        private ListObjectInspector internalMergeObjectInspector;
        /** id key constant. */
        private static final String ID = "id";

        /** list key constant. */
        private static final String LIST = "list";

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            // Setup the object inspectors and return type
            switch (mode) {
                case PARTIAL1 :
                    // Get the object inspectors
                    idObjectInspector = parameters[0];
                    listObjectInspector = (ListObjectInspector) parameters[1];

                    // The field names for the struct, order matters
                    List<String> fieldNames = Arrays.asList(ID, LIST);

                    // The field inspectors for the struct, order matters
                    List<ObjectInspector> fieldInspectors = Arrays.asList(idObjectInspector, listObjectInspector)
                            .stream().map(ObjectInspectorUtils::getStandardObjectInspector)
                            .map(ObjectInspectorFactory::getStandardListObjectInspector).collect(Collectors.toList());

                    // Will output structs
                    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
                case PARTIAL2 :
                    // Get the struct object inspector
                    internalMergeObjectInspector = (ListObjectInspector) parameters[0];

                    // Will output structs
                    return internalMergeObjectInspector;
                case FINAL :
                    // Get the struct object inspector
                    internalMergeObjectInspector = (ListObjectInspector) parameters[0];

                    // Will output list

                    return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory
                            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
                case COMPLETE :
                    // Get the object inspectors
                    idObjectInspector = parameters[0];
                    listObjectInspector = (ListObjectInspector) parameters[1];

                    // Will output list
                    return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory
                            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
                default :
                    throw new HiveException("Unknown Mode: " + mode.toString());
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new PreprocessFunnelMergeAggregateBuffer();
        }

        @Override
        public void iterate(AggregationBuffer aggregate, Object[] parameters) throws HiveException {
            PreprocessFunnelMergeAggregateBuffer buffer = (PreprocessFunnelMergeAggregateBuffer) aggregate;

            Object id = parameters[0];
            // Get the id value
            Object idValue = ObjectInspectorUtils.copyToStandardObject(id, idObjectInspector);

            Object funnelList = parameters[1];
            List<Long> funnelListValue = ((List<Object>) listObjectInspector.getList(funnelList)).stream()
                    .map(people -> (long) people).collect(Collectors.toList());

            buffer.addFunnel(String.valueOf(id), funnelListValue);

        }

        @SuppressWarnings("unchecked")
        @Override
        public void merge(AggregationBuffer aggregate, Object partial) throws HiveException {
            if (partial != null) {

                // Get the funnel aggregate and the funnel data
                PreprocessFunnelMergeAggregateBuffer funnelAggregate = (PreprocessFunnelMergeAggregateBuffer) aggregate;

                // Convert the partial results into a list of longs
                List<List<String>> result = (List<List<String>>) listObjectInspector.getList(partial);

                List<List<String>> elements = funnelAggregate.getElements();

                // combine
                for (int i = 0; i < elements.size(); i++) {
                    elements.get(i).addAll(result.get(i));
                    elements.set(i, new ArrayList<String>(((HashSet<String>) elements.get(i))));
                }
                funnelAggregate.setElements(elements);
            }
        }

        @Override
        public void reset(AggregationBuffer aggregate) throws HiveException {
            PreprocessFunnelMergeAggregateBuffer funnelAggregate = (PreprocessFunnelMergeAggregateBuffer) aggregate;
            funnelAggregate.clear();
        }

        @Override
        public Object terminate(AggregationBuffer aggregate) throws HiveException {
            PreprocessFunnelMergeAggregateBuffer funnelAggregate = (PreprocessFunnelMergeAggregateBuffer) aggregate;
            return funnelAggregate.output();
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregate) throws HiveException {
            PreprocessFunnelMergeAggregateBuffer funnelAggregate = (PreprocessFunnelMergeAggregateBuffer) aggregate;
            return funnelAggregate.getElements();
        }
    }
}
