package com.spark.poc.pojo;

public class Employee {
	public Employee(String name, String age, String job) {
		super();
		this.name = name;
		this.age = age;
		this.job = job;
	}

	public String name;
	public String age;
	public String job;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public String getJob() {
		return job;
	}

	public void setJob(String job) {
		this.job = job;
	}

}
