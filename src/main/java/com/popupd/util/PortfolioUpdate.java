package com.popupd.util;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class PortfolioUpdate extends KeyedCoProcessFunction<String, StockReturn, PortfolioStatsSchema, PortfolioStatsSchema> {

    private transient ValueState<PortfolioStatsSchema> latestStats;

    @Override
    public void open(Configuration config) {
        TypeInformation<PortfolioStatsSchema> typeInfo = TypeInformation.of(PortfolioStatsSchema.class);
        ValueStateDescriptor<PortfolioStatsSchema> statsDescriptor =
                new ValueStateDescriptor<>("latestStats", typeInfo);
        latestStats = getRuntimeContext().getState(statsDescriptor);
    }

    @Override
    public void processElement1(
            StockReturn stockReturn,
            KeyedCoProcessFunction<String, StockReturn, PortfolioStatsSchema, PortfolioStatsSchema>.Context context,
            Collector<PortfolioStatsSchema> collector
    ) throws Exception {
        PortfolioStatsSchema currentStats = latestStats.value();
        if (currentStats != null) {
            PortfolioStatsSchema updatedStats = updateStats(currentStats, stockReturn);
            latestStats.update(updatedStats);
            collector.collect(updatedStats);
        }
    }

    @Override
    public void processElement2(
            PortfolioStatsSchema incomingStats,
            KeyedCoProcessFunction<String, StockReturn, PortfolioStatsSchema, PortfolioStatsSchema>.Context context,
            Collector<PortfolioStatsSchema> collector
    ) throws Exception {
        latestStats.update(incomingStats);
        collector.collect(incomingStats);
    }

    private PortfolioStatsSchema updateStats(PortfolioStatsSchema stats, StockReturn ret) {
        String symbol = ret.getTicker();
        double retValue = ret.getReturnValue();

        Map<CharSequence, Double> meanReturns = new HashMap<>(stats.getMeanReturns());
        Map<CharSequence, Map<CharSequence, Double>> covMatrix = deepCopyCovMatrix(stats.getCovarianceMatrix());

        int count = 100;  // or compute from timestamp if needed
        int newCount = count + 1;

        // Step 1: Old mean
        double oldMean = meanReturns.getOrDefault(symbol, 0.0);

        // Step 2: Update mean
        double newMean = oldMean + (retValue - oldMean) / newCount;
        meanReturns.put(symbol, newMean);

        // Step 3: Update covariance matrix
        for (CharSequence otherSymbol : meanReturns.keySet()) {
            double otherMean = meanReturns.get(otherSymbol);
            double covOld = covMatrix.getOrDefault(symbol, new HashMap<>())
                    .getOrDefault(otherSymbol, 0.0);

            // Return of other asset — assume 0 if no return yet (or keep previous)
            double retOther = 0.0;
            if (symbol.equals(otherSymbol)) {
                retOther = retValue;  // self-covariance
            } else {
                // estimate using old mean if no current return
                retOther = otherMean;
            }

            double deltaCov = ((retValue - oldMean) * (retOther - otherMean) - covOld) / newCount;
            double covNew = covOld + deltaCov;

            // symmetric matrix
            covMatrix.computeIfAbsent(symbol, k -> new HashMap<>()).put(otherSymbol, covNew);
            covMatrix.computeIfAbsent(otherSymbol, k -> new HashMap<>()).put(symbol, covNew);
        }

        PortfolioStatsSchema updated = new PortfolioStatsSchema();
        updated.setMeanReturns(meanReturns);
        updated.setCovarianceMatrix(covMatrix);
        updated.setTimestamp(Instant.now().toEpochMilli());


        return updated;
    }

    private Map<CharSequence, Map<CharSequence, Double>> deepCopyCovMatrix(Map<CharSequence, Map<CharSequence, Double>> original) {
        Map<CharSequence, Map<CharSequence, Double>> copy = new HashMap<>();
        for (Map.Entry<CharSequence, Map<CharSequence, Double>> entry : original.entrySet()) {
            Map<CharSequence, Double> innerCopy = new HashMap<>(entry.getValue());
            copy.put(entry.getKey(), innerCopy);
        }
        return copy;
    }
}
