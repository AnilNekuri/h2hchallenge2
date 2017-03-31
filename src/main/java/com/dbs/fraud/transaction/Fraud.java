package com.dbs.fraud.transaction;

public class Fraud {

	private boolean isFraud;
	private String reason;
	
	public boolean isFraud() {
		return isFraud;
	}
	public void setFraud(boolean isFraud) {
		this.isFraud = isFraud;
	}
	public String getReason() {
		return reason;
	}
	public void setReason(String reason) {
		this.reason = reason;
	}
	
}
