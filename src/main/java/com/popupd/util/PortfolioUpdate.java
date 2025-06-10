package com.popupd.util;

import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PortfolioUpdate extends KeyedCoProcessFunction<String, StockReturn, PortfolioStatsSchema, PortfolioStatsSchema> {

    private transient ValueState<PortfolioStatsSchema> globalStats;

    @Override
    public void open(Configuration config) {
        TypeInformation<PortfolioStatsSchema> typeInfo = TypeInformation.of(PortfolioStatsSchema.class);
        ValueStateDescriptor<PortfolioStatsSchema> statsDescriptor =
                new ValueStateDescriptor<>("globalPortfolioStats", typeInfo);

        globalStats = getRuntimeContext().getState(statsDescriptor);
    }

    @Override
    public void processElement1(
            StockReturn stockReturn,
            Context context,
            Collector<PortfolioStatsSchema> collector
    ) throws Exception {
        PortfolioStatsSchema currentStats = globalStats.value();

        // Initialize the state if it's null
        if (currentStats == null) {
            return;
        }

        // Update the stats
        synchronized (currentStats) {

            PortfolioStatsSchema updatedStats = updateStats(currentStats, stockReturn);

            // Update the state
            globalStats.update(updatedStats);

            // Optionally, produce the updated stats downstream (if needed)
            collector.collect(updatedStats);
        }
    }

    @Override
    public void processElement2(PortfolioStatsSchema newStats, Context ctx, Collector<PortfolioStatsSchema> out) throws Exception {
        globalStats.update(newStats);

    }

    private PortfolioStatsSchema updateStats(PortfolioStatsSchema stats, StockReturn ret) {
        String symbol = ret.getTicker();
        double retValue = ret.getReturnValue();
        Utf8 utf8Symbol = new Utf8(symbol);
        Map<CharSequence, Double> meanReturns = stats.getMeanReturns();
        Map<CharSequence, Map<CharSequence, Double>> covMatrix = stats.getCovarianceMatrix();

        int count = 100;
        int newCount = count + 1;

        double oldMean = meanReturns.getOrDefault(utf8Symbol, 0.0);
        double newMean = oldMean + (retValue - oldMean) / newCount;


        if(oldMean != 0.0 ) {
            meanReturns.put(symbol, newMean);
        }



        for (CharSequence otherSymbol : meanReturns.keySet()) {
            Utf8 utf8otherSymbol = new Utf8(otherSymbol.toString());



            double otherMean = meanReturns.getOrDefault(utf8otherSymbol, 0.0);
            double covOld = covMatrix.getOrDefault(symbol, new HashMap<>())
                    .getOrDefault(otherSymbol, 0.0);


            double retOther = symbol.contentEquals(otherSymbol) ? retValue : otherMean;

            double deltaCov = ((retValue - oldMean) * (retOther - otherMean) - covOld) / newCount;
            double covNew = covOld + deltaCov;

            if(covNew != 0.0 && !Double.isNaN(covNew)) {
                covMatrix.computeIfAbsent(symbol, k -> new HashMap<>()).put(otherSymbol, covNew);
                covMatrix.computeIfAbsent(otherSymbol, k -> new HashMap<>()).put(symbol, covNew);
            }
        }

        // Update the stats object directly
        stats.setMeanReturns(meanReturns);
        stats.setCovarianceMatrix(covMatrix);
        stats.setTimestamp(Instant.now().toEpochMilli());

        return stats;
    }

//    private Map<CharSequence, Map<CharSequence, Double>> deepCopyCovMatrix(Map<CharSequence, Map<CharSequence, Double>> original) {
//        Map<CharSequence, Map<CharSequence, Double>> copy = new HashMap<>();
//        for (Map.Entry<CharSequence, Map<CharSequence, Double>> entry : original.entrySet()) {
//            Map<CharSequence, Double> innerCopy = new HashMap<>(entry.getValue());
//            copy.put(entry.getKey(), innerCopy);
//        }
//        return copy;
//    }
}
