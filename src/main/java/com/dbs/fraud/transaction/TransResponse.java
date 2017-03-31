package com.dbs.fraud.transaction;

public class TransResponse {

	private String status;
	private String msg;
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getMsg() {
		return msg;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}
	public TransResponse(String status, String msg) {
		super();
		this.status = status;
		this.msg = msg;
	}
	
}
