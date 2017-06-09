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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

@UDFType(deterministic = true)
@Description(name = "funnel", value = "_FUNC_(action_column, timestamp_column, step_1, step_2, ...) - Builds a funnel report applied to the action_column. Steps are arrays of the same type as action. Should be used with merge_funnel UDF.", extended = "Example: SELECT funnel(action, timestamp, array('signup_page', 'email_signup'), \n"
        + "                                          array('confirm_button'),\n"
        + "                                          array('submit_button')) AS funnel\n" + "         FROM table\n"
        + "         GROUP BY user_id;")
public class Funnel extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(Funnel.class.getName());

    @Override
    public FunnelEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        // Get the parameters
        TypeInfo[] parameters = info.getParameters();

        // Check number of arguments
        if (parameters.length < 3) {
            throw new UDFArgumentLengthException(
                    "Please specify the action column, the timestamp column, and at least one funnel.");
        }

        // check the groupby list
        if (parameters[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(0,
                    "Only list type arguments are accepted but " + parameters[0].getTypeName() + " was passed.");
        }

        // Check the action_column type and enforce that all funnel steps are
        // the same type
        if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(1,
                    "Only primitive type arguments are accepted but " + parameters[1].getTypeName() + " was passed.");
        }
        PrimitiveCategory actionColumnCategory = ((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory();

        // Check the timestamp_column type
        if (parameters[2].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(2,
                    "Only primitive type arguments are accepted but " + parameters[2].getTypeName() + " was passed.");
        }

        // Check the conversion_period column type
        if (parameters[3].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(3,
                    "Only primitive type arguments are accepted but " + parameters[23].getTypeName() + " was passed.");
        }

        // Check that all funnel steps are the same type as the action_column
        for (int i = 4; i < parameters.length; i++) {
            switch (parameters[i].getCategory()) {
                case LIST :
                    // Check that the list is of primitives of the same type as
                    // the action column
                    TypeInfo typeInfo = ((ListTypeInfo) parameters[i]).getListElementTypeInfo();
                    if (typeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE
                            || ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory() != actionColumnCategory) {
                        throw new UDFArgumentTypeException(i,
                                "Funnel list parameter " + Integer.toString(i) + " of type "
                                        + parameters[i].getTypeName() + " does not match expected type "
                                        + parameters[0].getTypeName() + ".");
                    }
                    break;
                default :
                    throw new UDFArgumentTypeException(i, "Funnel list parameter " + Integer.toString(i) + " of type "
                            + parameters[i].getTypeName() + " should be a list.");
            }
        }

        return new FunnelEvaluator();
    }

    public static class FunnelEvaluator extends GenericUDAFEvaluator {
        /** For PARTIAL1 and COMPLETE. */
        private ListObjectInspector groupObjectInspector;

        /** For PARTIAL1 and COMPLETE. */
        private ObjectInspector actionObjectInspector;

        /** For PARTIAL1 and COMPLETE. */
        private ObjectInspector timestampObjectInspector;

        /** For PARTIAL1 and COMPLETE. */
        private ListObjectInspector funnelObjectInspector;

        /** For PARTIAL1 and COMPLETE. */
        private ObjectInspector conversionPeriodObjectInspector;

        /** For PARTIAL2 and FINAL. */
        private StandardStructObjectInspector internalMergeObjectInspector;

        /** Group key constant. */
        private static final String GROUP = "group";

        /** Action key constant. */
        private static final String ACTION = "action";

        /** Timestamp key constant. */
        private static final String TIMESTAMP = "timestamp";

        /** Period key constant. */
        private static final String Period = "period";

        /** Funnel key constant. */
        private static final String FUNNEL = "funnel";

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            // Setup the object inspectors and return type
            switch (m) {
                case PARTIAL1 :
                    // Get the object inspectors
                    groupObjectInspector = (ListObjectInspector) parameters[0];
                    actionObjectInspector = parameters[1];
                    timestampObjectInspector = parameters[2];
                    conversionPeriodObjectInspector = parameters[3];
                    funnelObjectInspector = (ListObjectInspector) parameters[4];

                    // The field names for the struct, order matters
                    List<String> fieldNames = Arrays.asList(GROUP, ACTION, TIMESTAMP, Period, FUNNEL);

                    // The field inspectors for the struct, order matters
                    List<ObjectInspector> fieldInspectors = Arrays
                            .asList(groupObjectInspector, actionObjectInspector, timestampObjectInspector,
                                    conversionPeriodObjectInspector, funnelObjectInspector)
                            .stream().map(ObjectInspectorUtils::getStandardObjectInspector)
                            .map(ObjectInspectorFactory::getStandardListObjectInspector).collect(Collectors.toList());

                    // Will output structs
                    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
                case PARTIAL2 :
                    // Get the struct object inspector
                    internalMergeObjectInspector = (StandardStructObjectInspector) parameters[0];

                    // Will output structs
                    return internalMergeObjectInspector;
                case FINAL :
                    // Get the struct object inspector
                    internalMergeObjectInspector = (StandardStructObjectInspector) parameters[0];

                    // Will output map , key:String value: List
                    return ObjectInspectorFactory.getStandardMapObjectInspector(
                            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                            ObjectInspectorFactory.getStandardListObjectInspector(
                                    PrimitiveObjectInspectorFactory.javaLongObjectInspector));
                case COMPLETE :
                    // Get the object inspectors
                    groupObjectInspector = (ListObjectInspector) parameters[0];
                    actionObjectInspector = parameters[1];
                    timestampObjectInspector = parameters[2];
                    conversionPeriodObjectInspector = parameters[3];
                    funnelObjectInspector = (ListObjectInspector) parameters[4];

                    // Will output map , key:String value: List
                    return ObjectInspectorFactory.getStandardMapObjectInspector(
                            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                            ObjectInspectorFactory.getStandardListObjectInspector(
                                    PrimitiveObjectInspectorFactory.javaLongObjectInspector));
                default :
                    throw new HiveException("Unknown Mode: " + m.toString());
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new FunnelAggregateBuffer();
        }

        /**
         * Adds funnel steps to the aggregate. Funnel steps can be lists or
         * scalars.
         *
         * @param funnelAggregate
         * @param parameters
         */
        private void addFunnelSteps(FunnelAggregateBuffer funnelAggregate, Object[] parameters) {
            Arrays.stream(parameters).map(this::convertFunnelStepObjectToList).map(ListUtils::removeNullFromList)
                    .filter(ListUtils::isNotEmpty).forEach(funnelStep -> {
                        funnelAggregate.funnelSteps.add(new HashSet<Object>(funnelStep));
                        funnelAggregate.funnelSet.addAll(funnelStep);
                    });
        }

        @Override
        public void iterate(AggregationBuffer aggregate, Object[] parameters) throws HiveException {
            FunnelAggregateBuffer funnelAggregate = (FunnelAggregateBuffer) aggregate;

            // Add the funnel steps if not already stored
            if (funnelAggregate.funnelSteps.isEmpty()) {
                // Funnel steps start at index 3
                addFunnelSteps(funnelAggregate, Arrays.copyOfRange(parameters, 4, parameters.length));

                Object conversionPeriod = parameters[3];
                // Get the conversionPeriod value
                Object conversionPeriodValue = ObjectInspectorUtils.copyToStandardObject(conversionPeriod,
                        conversionPeriodObjectInspector);
                // 漏斗转换周期
                funnelAggregate.setConversionPeriod(((IntWritable) conversionPeriodValue).get());
            }

            // Get the action_column value and add it (if it matches a funnel)
            Object group = parameters[0];
            Object action = parameters[1];
            Object timestamp = parameters[2];
            if (action != null && timestamp != null) {
                // Get the group value
                List<Object> groupList = convertFunnelStepObjectToList(group);
                // 合并维度
                String groupValue = "总体";
                if (groupList.size() != 0) {
                    groupValue = groupList.stream().map(String::valueOf)
                            .reduce((result, element) -> result + " " + element).get();
                }

                // Get the action value
                Object actionValue = ObjectInspectorUtils.copyToStandardObject(action, actionObjectInspector);
                // Get the timestamp value
                Object timestampValue = ObjectInspectorUtils.copyToStandardObject(timestamp, timestampObjectInspector);

                // If the action is not null and it is one of the funnels we are
                // looking for, keep it
                if (actionValue != null && timestampValue != null && funnelAggregate.funnelSet.contains(actionValue)) {
                    funnelAggregate.actions.add(actionValue);
                    funnelAggregate.timestamps.add(timestampValue);
                    funnelAggregate.groups.add(groupValue);
                }
            }
        }

        /**
         * Given a struct and a key, look the key up in the struct with the
         * merge object inspector.
         *
         * @param object
         *            Struct object
         * @param key
         *            Key to look up
         */
        private Object structLookup(Object object, String key) {
            return internalMergeObjectInspector.getStructFieldData(object,
                    internalMergeObjectInspector.getStructFieldRef(key));
        }

        @Override
        public void merge(AggregationBuffer aggregate, Object partial) throws HiveException {
            FunnelAggregateBuffer funnelAggregate = (FunnelAggregateBuffer) aggregate;

            // Lists for partial data
            List<String> partialGroupList = ListUtils.toList(structLookup(partial, GROUP)).stream().map(String::valueOf)
                    .collect(Collectors.toList());
            List<Object> partialActionList = ListUtils.toList(structLookup(partial, ACTION));
            List<Object> partialTimestampList = ListUtils.toList(structLookup(partial, TIMESTAMP));

            // If we don't have any funnel steps stored, then we should copy the
            // funnel steps from the partial list
            if (funnelAggregate.funnelSteps.isEmpty()) {
                List<Object> partialFunnelList = ListUtils.toList(structLookup(partial, FUNNEL));
                funnelAggregate.deserializeFunnel(partialFunnelList);
            }

            // Add all the partial actions and timestamps to the end of the
            // lists
            funnelAggregate.groups.addAll(partialGroupList);
            funnelAggregate.actions.addAll(partialActionList);
            funnelAggregate.timestamps.addAll(partialTimestampList);

            funnelAggregate.setConversionPeriod((int) structLookup(partial, "period"));
        }

        @Override
        public void reset(AggregationBuffer aggregate) throws HiveException {
            FunnelAggregateBuffer funnelAggregate = (FunnelAggregateBuffer) aggregate;
            funnelAggregate.clear();
        }

        @Override
        public Object terminate(AggregationBuffer aggregate) throws HiveException {
            FunnelAggregateBuffer funnelAggregate = (FunnelAggregateBuffer) aggregate;
            return funnelAggregate.computeFunnel();
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregate) throws HiveException {
            FunnelAggregateBuffer funnelAggregate = (FunnelAggregateBuffer) aggregate;
            return funnelAggregate.serialize();
        }

        /**
         * Convert object to list of funnels for a funnel step.
         *
         * @parameter
         * @return List of funnels in funnel step
         */
        private List<Object> convertFunnelStepObjectToList(Object parameter) {
            if (parameter instanceof List) {
                return (List<Object>) funnelObjectInspector.getList(parameter);
            } else {
                return Arrays.asList(ObjectInspectorUtils.copyToStandardObject(parameter,
                        funnelObjectInspector.getListElementObjectInspector()));
            }
        }

    }
}
