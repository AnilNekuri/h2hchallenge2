package com.dbs.fraud.transaction;

public class Transaction {
	private String id;
	private String deviceId;
	private double transactionValue;
	private String accountId;
	private long tsMillis;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getDeviceId() {
		return deviceId;
	}
	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}
	public double getTransactionValue() {
		return transactionValue;
	}
	public void setTransactionValue(double transactionValue) {
		this.transactionValue = transactionValue;
	}
	public String getAccountId() {
		return accountId;
	}
	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}
	public long getTsMillis() {
		return tsMillis;
	}
	public void setTsMillis(long tsMillis) {
		this.tsMillis = tsMillis;
	}
	
}
