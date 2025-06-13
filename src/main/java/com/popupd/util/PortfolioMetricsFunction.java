package com.popupd.util;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class PortfolioMetricsFunction extends KeyedCoProcessFunction<String, WeightStockSchema, PortfolioStatsSchema, PortfolioMetrics> {

    private transient ValueState<PortfolioStatsSchema> portfolioStatsState;

    private transient ValueState<WeightStockSchema> weightStockState;

    @Override
    public void open(Configuration config) throws Exception {

        TypeInformation<PortfolioStatsSchema> statsTypeInfo = TypeInformation.of(PortfolioStatsSchema.class);
        ValueStateDescriptor<PortfolioStatsSchema> statsDescriptor =
                new ValueStateDescriptor<>("portfolioStats", statsTypeInfo);
        portfolioStatsState = getRuntimeContext().getState(statsDescriptor);

        TypeInformation<WeightStockSchema> weightTypeInfo = TypeInformation.of(WeightStockSchema.class);
        ValueStateDescriptor<WeightStockSchema> weightDescriptor =
                new ValueStateDescriptor<>("weightStock", weightTypeInfo);
        weightStockState = getRuntimeContext().getState(weightDescriptor);
    }

    @Override
    public void processElement1(WeightStockSchema weightStockSchema, KeyedCoProcessFunction<String, WeightStockSchema, PortfolioStatsSchema, PortfolioMetrics>.Context context, Collector<PortfolioMetrics> collector) throws Exception {
        weightStockState.update(weightStockSchema);
        PortfolioStatsSchema currentStats = portfolioStatsState.value();

        if (currentStats == null) {
            return;
        }

        PortfolioMetrics updatedMetrics = calculateMetrics(weightStockSchema, currentStats);
        collector.collect(updatedMetrics);

    }

    private PortfolioMetrics calculateMetrics(WeightStockSchema weightStockSchema, PortfolioStatsSchema currentStats) {
        return new PortfolioMetrics();
    }

    @Override
    public void processElement2(PortfolioStatsSchema portfolioStatsSchema, KeyedCoProcessFunction<String, WeightStockSchema, PortfolioStatsSchema, PortfolioMetrics>.Context context, Collector<PortfolioMetrics> collector) throws Exception {
        portfolioStatsState.update(portfolioStatsSchema);
        WeightStockSchema currentWeight = weightStockState.value();

        if (currentWeight == null){
            return;
        }

        PortfolioMetrics updatedMetrics = calculateMetrics(currentWeight, portfolioStatsSchema);
        collector.collect(updatedMetrics);

    }
}
