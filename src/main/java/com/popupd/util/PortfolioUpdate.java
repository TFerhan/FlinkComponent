package com.popupd.util;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class PortfolioUpdate extends KeyedProcessFunction<String, StockReturn, PortfolioStatsSchema> {

    private transient ValueState<PortfolioStatsSchema> globalStats;

    @Override
    public void open(Configuration config) {
        TypeInformation<PortfolioStatsSchema> typeInfo = TypeInformation.of(PortfolioStatsSchema.class);
        ValueStateDescriptor<PortfolioStatsSchema> statsDescriptor =
                new ValueStateDescriptor<>("globalPortfolioStats", typeInfo);
        globalStats = getRuntimeContext().getState(statsDescriptor);
    }

    @Override
    public void processElement(
            StockReturn stockReturn,
            Context context,
            Collector<PortfolioStatsSchema> collector
    ) throws Exception {
        PortfolioStatsSchema currentStats = globalStats.value();

        // Initialize the state if it's null
        if (currentStats == null) {
            currentStats = new PortfolioStatsSchema();
            currentStats.setMeanReturns(new HashMap<>());
            currentStats.setCovarianceMatrix(new HashMap<>());
            currentStats.setTimestamp(Instant.now().toEpochMilli());
            currentStats.setPortfolioId("portfolio-1");
        }

        // Update the stats
        PortfolioStatsSchema updatedStats = updateStats(currentStats, stockReturn);

        // Update the state
        globalStats.update(updatedStats);

        // Optionally, produce the updated stats downstream (if needed)
        collector.collect(updatedStats);
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

            double retOther = symbol.equals(otherSymbol) ? retValue : otherMean;

            double deltaCov = ((retValue - oldMean) * (retOther - otherMean) - covOld) / newCount;
            double covNew = covOld + deltaCov;

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
