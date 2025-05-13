package com.popupd.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import com.popupd.util.PortfolioCumulativeReturn;

public class CumulativeWeightedReturnFunction
        extends KeyedProcessFunction<String, WeightedReturn, PortfolioCumulativeReturn> {

    private ValueState<Double> cumulativeWeightedReturn;

    @Override
    public void open(Configuration parameters) {

        ValueStateDescriptor<Double> descriptor =
                new ValueStateDescriptor<>("cumulativeWeightedReturn", Double.class);
        cumulativeWeightedReturn = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(WeightedReturn value,
                               Context ctx,
                               Collector<PortfolioCumulativeReturn> out) throws Exception {

        Double currentSum = cumulativeWeightedReturn.value();
        if(currentSum == null) {
            currentSum = 0.0;
        }
        currentSum += value.getWeightedReturn();
        cumulativeWeightedReturn.update(currentSum);
        out.collect(new PortfolioCumulativeReturn(currentSum, value.getTimestamp()));
    }
}
