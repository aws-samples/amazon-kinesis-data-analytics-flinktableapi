package com.amazonaws.services.kinesisanalytics;
import java.sql.Timestamp;

public class Order {
    public int id;
    public Timestamp orderTime;
    public int amount;
    public String currency;
}
