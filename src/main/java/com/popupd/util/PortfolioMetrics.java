package com.popupd.util;

public class PortfolioMetrics {

    private double risk;
    private double expectedReturn;
    private double sharpRatio;

    private long timestamp;

    public PortfolioMetrics(double risk, double expectedReturn, double sharpRatio, long timestamp) {
        this.risk = risk;
        this.expectedReturn = expectedReturn;
        this.sharpRatio = sharpRatio;
        this.timestamp = timestamp;
    }

    public PortfolioMetrics() {
        this.risk = 0.0;
        this.expectedReturn = 0.0;
        this.sharpRatio = 0.0;
        this.timestamp = System.currentTimeMillis();
    }

    public double getRisk() {
        return risk;
    }

    public double getExpectedReturn() {
        return expectedReturn;
    }

    public double getSharpRatio() {
        return sharpRatio;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setRisk(double risk) {
        this.risk = risk;
    }

    public void setExpectedReturn(double expectedReturn) {
        this.expectedReturn = expectedReturn;
    }

    public void setSharpRatio(double sharpRatio) {
        this.sharpRatio = sharpRatio;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "PortfolioMetrics{" +
                "risk=" + risk +
                ", expectedReturn=" + expectedReturn +
                ", sharpRatio=" + sharpRatio +
                ", timestamp=" + timestamp +
                '}';
    }




}
