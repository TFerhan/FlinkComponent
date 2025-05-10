package com.popupd.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class StockReturn {
    private String ticker;
    private double returnValue;
    private String timestamp;

    public StockReturn() {}

    public StockReturn(String ticker, double returnValue, String timestamp) {
        this.ticker = ticker;
        this.returnValue = returnValue;
        this.timestamp = timestamp;
    }

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public double getReturnValue() {
        return returnValue;
    }

    public void setReturnValue(double returnValue) {
        this.returnValue = returnValue;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String charSequence) {
        this.timestamp = charSequence;
    }

    @Override
    public String toString() {
        return "StockReturn{" +
                "ticker='" + ticker + '\'' +
                ", returnValue=" + returnValue +
                ", timestamp=" + timestamp +
                '}';
    }
}
