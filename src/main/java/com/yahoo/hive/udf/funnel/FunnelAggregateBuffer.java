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

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Used to build funnel.
 */
class FunnelAggregateBuffer implements AggregationBuffer {

    private static final DateTimeFormatter IN_HOUR_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    /** List of groups. */
    List<String> groups = new ArrayList<>();

    /** List of actions. */
    List<Object> actions = new ArrayList<>();

    /** List of timestamps associated with actions. */
    List<Object> timestamps = new ArrayList<>();

    /** List of funnel steps. Funnel steps can have multiple funnels. */
    List<Set<Object>> funnelSteps = new ArrayList<Set<Object>>();

    /** Set of all funnels we are looking for. */
    Set<Object> funnelSet = new HashSet<>();

    // 漏斗转换周期,以小时为单位
    int conversionPeriod = 0;

    public int getConversionPeriod() {
        return conversionPeriod;
    }

    public void setConversionPeriod(int conversionPeriod) {
        this.conversionPeriod = conversionPeriod;
    }

    /**
     * Serialize actions, timestamps, and funnel steps. Have to split funnel
     * steps with null. This is more efficient than passing around structs.
     *
     * @return List of objects
     */
    public List<Object> serialize() {
        List<Object> serialized = new ArrayList<>();
        serialized.add(groups);
        serialized.add(actions);
        serialized.add(timestamps);
        serialized.add(getConversionPeriod());

        // Need to do special logic for funnel steps
        List<Object> serializedFunnelSteps = new ArrayList<>();
        for (Set e : funnelSteps) {
            // Separate funnel steps with null
            serializedFunnelSteps.addAll(e);
            serializedFunnelSteps.add(null);
        }

        // Add the funnel steps
        serialized.add(serializedFunnelSteps);

        return serialized;
    }

    /**
     * Deserialize funnel steps. Have to deserialize the null separated list.
     */
    public void deserializeFunnel(List<Object> serializedFunnel) {
        // Have to "deserialize" from the null separated list
        Set<Object> funnelStepAccumulator = new HashSet<>();
        for (Object e : serializedFunnel) {
            // If not null
            if (e != null) {
                // Add to the step accumulator
                funnelStepAccumulator.add(e);
            } else {
                // Found a null, add the funnel step
                // Need to do a deep copy
                funnelSteps.add(new HashSet<>(funnelStepAccumulator));
                // Clear the set
                funnelStepAccumulator.clear();
            }
        }
    }

    /**
     * Clear the aggregate.
     */
    public void clear() {
        actions.clear();
        timestamps.clear();
    }

    /**
     * Compute the funnel. Sort the actions by timestamp/action, then build the
     * funnel.
     *
     * @return list of longs representing the funnel
     */
    public Map<String, List<Long>> computeFunnel() {

        // Input size
        int inputSize = actions.size();

        // The last funnel index
        int funnelStepSize = funnelSteps.size();

        // Result funnel, all 0's at the start
        Map<String, List<Long>> results = new HashMap<>();

        // 后续步骤的记录
        Map<String, List<String>> noGroupMap = new HashMap<>();
        // key为第一个步骤分组后的group key, vlaue为actionTimeMap
        Map<String, Map<String, List<String>>> groupMap = new HashMap<>();
        List<Long> result = new ArrayList<>(Collections.nCopies(funnelStepSize, 0L));
        String firstAction = (String) funnelSteps.get(0).stream().collect(Collectors.toList()).get(0);
        for (int i = 0; i < inputSize; i++) {
            String action = String.valueOf(actions.get(i));
            String time = String.valueOf(timestamps.get(i));
            String group = String.valueOf(groups.get(i));
            if (action.equals(firstAction)) {
                addGroupMap(groupMap, action, time, group);
            } else {
                addNoGroupMap(noGroupMap, action, time);
            }
        }

        if (groupMap.size() == 0) {
            results.put("总体", result);
            return results;
        }

        // 对后续步骤排序
        for (List<String> timestamps : noGroupMap.values()) {
            Collections.sort(timestamps);
        }

        for (Entry<String, Map<String, List<String>>> groupEntry : groupMap.entrySet()) {

            result = new ArrayList<>(Collections.nCopies(funnelStepSize, 0L));

            String group = groupEntry.getKey();
            Map<String, List<String>> actionTimeMap = groupEntry.getValue();
            // 将noGroupMap中的记录加到groupMap中每一个分组的actionTimeMap中
            actionTimeMap.putAll(noGroupMap);

            List<String> firstStepTimestamps = actionTimeMap.get(firstAction);
            if (null == firstStepTimestamps || firstStepTimestamps.size() == 0) {
                results.put(group, result);
                break;
            } else {
                result.set(0, 1L);
            }
            // 对第一步骤的发生时间排序
            Collections.sort(firstStepTimestamps);
            // 遍历第一步的时间 从（第二步的最小发生时间减去转换周期）开始遍历到（第二步的最大值）
            String secondStepAction = (String) funnelSteps.get(1).stream().collect(Collectors.toList()).get(0);
            List<String> secondStepTimestamps = actionTimeMap.get(secondStepAction);
            if (null == secondStepTimestamps || secondStepTimestamps.size() == 0) {
                results.put(group, result);
                break;
            }
            String minFirstStepTimestamp = DateTime.parse(secondStepTimestamps.get(0), IN_HOUR_FORMATTER)
                    .minusHours(conversionPeriod).toString(IN_HOUR_FORMATTER);
            if (minFirstStepTimestamp.compareTo(firstStepTimestamps.get(firstStepTimestamps.size() - 1)) > 0) {
                break;
            }
            int minFirstStepIndex = Math.abs(Collections.binarySearch(firstStepTimestamps, minFirstStepTimestamp) + 1);
            int maxFirstStepIndex = Math.abs(Collections.binarySearch(firstStepTimestamps,
                    secondStepTimestamps.get(secondStepTimestamps.size() - 1)) + 1);
            for (int i = minFirstStepIndex; i <= maxFirstStepIndex; i++) {
                if (!result.contains(0L)) {
                    break;
                }
                String firstTimestamp = firstStepTimestamps.get(i);
                result = checkFunnel(firstTimestamp, actionTimeMap, result);
            }

            results.put(group, result);
        }
        return results;
    }

    private List<Long> checkFunnel(String firstTimestamp, Map<String, List<String>> actionTimeMap, List<Long> result) {
        String currentStepMinTime = firstTimestamp;
        // 每一个步骤递增的最小值 最后只需要判断每个步骤的递增最小值是不是满足在转换周期范围内即可
        List<String> everyStepMinTime = new ArrayList<>();
        everyStepMinTime.add(firstTimestamp);
        for (int currentFunnelStep = 1; currentFunnelStep < funnelSteps.size(); currentFunnelStep++) {
            String currentStepAction = (String) funnelSteps.get(currentFunnelStep).stream().collect(Collectors.toList())
                    .get(0);
            List<String> currentStepTimestamps = actionTimeMap.get(currentStepAction);
            if (null == currentStepTimestamps || currentStepTimestamps.size() == 0) {
                break;
            }

            // 找比上一步步骤最小时间大一些的时间
            String minTimestamps = currentStepTimestamps.get(0);
            String maxTimestamps = currentStepTimestamps.get(currentStepTimestamps.size() - 1);
            if (minTimestamps.compareTo(currentStepMinTime) > 0) {
                // 如果当前步骤的最小值都比currentStepMinTime大,则直接返回当前步骤的最小值
                currentStepMinTime = minTimestamps;
                everyStepMinTime.add(minTimestamps);
                continue;
            }
            if (maxTimestamps.compareTo(currentStepMinTime) <= 0) {
                // 如果当前步骤的最大值都比currentStepMinTime小,则没有比currentStepMinTime大的值,直接break
                break;
            }
            int searchTimeIndex = Collections.binarySearch(currentStepTimestamps, currentStepMinTime);

            String searchTime = currentStepTimestamps.get(Math.abs(searchTimeIndex + 1));
            currentStepMinTime = searchTime;
            everyStepMinTime.add(searchTime);
        }

        if (everyStepMinTime.size() != 0) {
            // 在转换周期内的最大时间:为第一步最小值在加上转换周期
            String inPeriodMaxTime = DateTime.parse(everyStepMinTime.get(0), IN_HOUR_FORMATTER)
                    .plusHours(conversionPeriod).toString(IN_HOUR_FORMATTER);
            everyStepMinTime = everyStepMinTime.stream().filter(time -> inPeriodMaxTime.compareTo(time) > 0)
                    .collect(Collectors.toList());
            for (int i = 0; i < everyStepMinTime.size(); i++) {
                result.set(i, 1L);
            }
        }
        return result;
    }

    /**
     * @param groupMap
     * @param action
     * @param time
     * @param group
     */
    private void addGroupMap(Map<String, Map<String, List<String>>> groupMap, String action, String time,
            String group) {
        if (groupMap.containsKey(group)) {
            Map<String, List<String>> actionTimeMap = groupMap.get(group);
            addNoGroupMap(actionTimeMap, action, time);
        } else {
            Map<String, List<String>> actionTimeMap = new HashMap<>();
            addNoGroupMap(actionTimeMap, action, time);
            groupMap.put(group, actionTimeMap);
        }
    }

    /**
     * @param noGroupMap
     * @param action
     * @param time
     * @param group
     */
    private void addNoGroupMap(Map<String, List<String>> noGroupMap, String action, String time) {
        if (noGroupMap.containsKey(action)) {
            noGroupMap.get(action).add(time);
        } else {
            List<String> times = new ArrayList<>();
            times.add(time);
            noGroupMap.put(action, times);
        }
    }

    public static void main(String[] args) throws SQLException, IOException, PropertyVetoException {
        System.out.println(DateTime.now());
        FunnelAggregateBuffer buffer = new FunnelAggregateBuffer();
        buffer.groups = new ArrayList<String>();
        buffer.actions = new ArrayList<Object>();
        buffer.timestamps = new ArrayList<Object>();
        buffer.funnelSteps = new ArrayList<Set<Object>>();
        buffer.funnelSteps.add(new HashSet<Object>() {
            {
                add("972");
            }
        });
        buffer.funnelSteps.add(new HashSet<Object>() {
            {
                add("973");
            }
        });
        buffer.conversionPeriod = 1;

        String sql1 = "select `default_timestamp`,'972' as action from parquet.`hdfs://10.10.102.104:9000/cloud_go/parquet/user_defined_event/192.168.88.134/972` as t0 where (( t0.year = 2017 and t0.month = 4 and t0.day >= 24 ) or ( t0.year = 2017 and t0.month > 4 and t0.month < 5 ) or ( t0.year = 2017 and t0.month = 5 and t0.day <= 23 )) and t0.`default_isDuid`= 'false' and t0.`default_userId`='test1' ";
        String sql2 = "select `default_timestamp`,'973' as action from parquet.`hdfs://10.10.102.104:9000/cloud_go/parquet/user_defined_event/192.168.88.134/973` as t1 where ( ( t1.year = 2017 and t1.month = 4 and t1.day >= 24 ) or ( t1.year = 2017 and t1.month > 4 and t1.month < 5 ) or ( t1.year = 2017 and t1.month = 5 and t1.day <= 23 ) ) and t1.`default_isDuid`= 'false' and t1.`default_userId`='test1' ";
        Connection con = SparkSqlClient.getInstance().getConnection();
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(sql1);
        while (rs.next()) {
            buffer.timestamps.add(rs.getObject(1));
            buffer.actions.add(rs.getObject(2));
            buffer.groups.add("总体");
        }
        rs = st.executeQuery(sql2);
        while (rs.next()) {
            buffer.timestamps.add(rs.getObject(1));
            buffer.actions.add(rs.getObject(2));
            buffer.groups.add("总体");
        }
        // rs = st.executeQuery(sql3);
        // while (rs.next()) {
        // buffer.timestamps.add(rs.getObject(1));
        // buffer.actions.add(rs.getObject(2));
        // buffer.groups.add("总体");
        // }
        System.out.println(DateTime.now());
        for (Entry<String, List<Long>> entry : buffer.computeFunnel().entrySet()) {

            System.out.println(entry.getKey() + " :");
            entry.getValue().stream().forEach(System.out::println);
        }
        System.out.println(DateTime.now());
    }
}
