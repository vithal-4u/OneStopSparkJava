package com.spark.poc.pojo;

import java.util.Map;

public class EpamEmployeePOJO {
	private String USER_ID;
	private String USER_NAME;
	private String TEXT;
	//private Map<String, String> hashCount;
	public String getUSER_ID() {
		return USER_ID;
	}
	public void setUSER_ID(String uSER_ID) {
		USER_ID = uSER_ID;
	}
	public String getUSER_NAME() {
		return USER_NAME;
	}
	public void setUSER_NAME(String uSER_NAME) {
		USER_NAME = uSER_NAME;
	}
	public String getTEXT() {
		return TEXT;
	}
	public void setTEXT(String tEXT) {
		TEXT = tEXT;
	}
	/*
	 * public Map<String, String> getHashCount() { return hashCount; } public void
	 * setHashCount(Map<String, String> hashCount) { this.hashCount = hashCount; }
	 */
}
