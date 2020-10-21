package com.spark.poc.ntc;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.poc.pojo.Employee;
import com.spark.poc.utils.SparkUtils;


public class RDDToDataFrameOrDataset {

	public static void main(String[] args) throws AnalysisException {
		SparkSession session = SparkUtils.createSparkSession("RDDToDataFrameOrDataset");
		JavaRDD<String> textRDD= session.read().option("header", "true")
				.textFile("D:\\Work_Bench\\spark-master\\examples\\src\\main\\resources\\people.csv")
				.javaRDD();
		String header = textRDD.first();
		JavaRDD<String> employeeFilteredRDD = textRDD.filter((String row) -> {
			return !row.equals(header);
		});
		JavaRDD<Employee> employeeRDD = employeeFilteredRDD.map(new Function<String, Employee>() {
			public Employee call(String row) throws Exception {
				String[] data =row.split(",");
				System.out.println(data[0]+" "+ data[1]+" "+ data[2]);
				return new Employee(data[0], data[1], data[2]);
			}
		});
		Dataset<Row> data = session.createDataFrame(employeeRDD, Employee.class);
		System.out.println("-------Group by Job----------");
		data.groupBy("job").count().show();
		data.show(10);
		/*data.createTempView("TestEmployee");
		Dataset<Row> dataframe = session.sql("select * from TestEmployee where age=25");
		//dataframe.show(5);
		Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
		Dataset<Employee> employeeDataset = dataframe.map(new MapFunction<Row, Employee>() {
			public Employee call(Row row) throws Exception {
				// TODO Auto-generated method stub
				return new Employee(row.getString(0), row.getString(1), row.getString(2));
			}
			},employeeEncoder);
		employeeDataset.show();*/
	}

}
