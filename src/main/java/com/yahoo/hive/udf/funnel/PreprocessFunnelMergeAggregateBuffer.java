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
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;

/**
 * Merges funnels into an aggregate.
 */
class PreprocessFunnelMergeAggregateBuffer implements AggregationBuffer {
    /** Stores funnel aggregate. */
    List<List<String>> elements = new ArrayList<>();

    public List<List<String>> getElements() {
        return elements;
    }

    public void setElements(List<List<String>> elements) {
        this.elements = elements;
    }

    /**
     * Add a funnel to the aggregate.
     *
     * @param funnel
     *            Funnel in the form of a list of longs.
     */
    public void addFunnel(String id, List<Long> funnel) throws HiveException {
        if (elements.isEmpty()) {
            for (int i = 0; i < funnel.size(); i++) {
                elements.add(new ArrayList<>());
            }
        }
        for (int i = 0; i < funnel.size(); i++) {
            if ((funnel.get(i) == 1) && !elements.get(i).contains(id)) {
                elements.get(i).add(id);
            }
        }
    }

    /**
     * Clear the aggregate.
     */
    public void clear() {
        elements.clear();
    }

    /**
     * Output aggregate. Must do a deep copy, and return a new list.
     *
     * @return Funnel aggregate counts.
     */
    public List<List<String>> output() {
        return this.elements;
    }

    public static void main(String[] args) throws Exception {
        new PreprocessFunnelMergeAggregateBuffer().addFunnel("未知", Arrays.asList(1L, 0L, 0L));
    }
}
