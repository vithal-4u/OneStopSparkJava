package com.spark.poc.pojo;

import java.io.Serializable;
import java.sql.Timestamp;

public class Players implements Serializable {
	String userId;
	String voting_PlayerId;
	String voting_playerName;
	String votedTime;
	
	public Players(String userId, String voting_PlayerId,
			String voting_playerName, String votedTime) {
		super();
		this.userId = userId;
		this.voting_PlayerId = voting_PlayerId;
		this.voting_playerName = voting_playerName;
		this.votedTime = votedTime;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getVoting_PlayerId() {
		return voting_PlayerId;
	}
	public void setVoting_PlayerId(String voting_PlayerId) {
		this.voting_PlayerId = voting_PlayerId;
	}
	public String getVoting_playerName() {
		return voting_playerName;
	}
	public void setVoting_playerName(String voting_playerName) {
		this.voting_playerName = voting_playerName;
	}
	public String getVotedTime() {
		return votedTime;
	}
	public void setVotedTime(String votedTime) {
		this.votedTime = votedTime;
	}
	
}
