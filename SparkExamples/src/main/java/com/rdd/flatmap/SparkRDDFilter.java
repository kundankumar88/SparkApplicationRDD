package com.rdd.flatmap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkRDDFilter {
	
	//Filter is used to remove Junk Values from RDD. when filter will return true that means it will be added to the rDD else it will not be added to 
	//the RDD 
	

	public static void main(String[] args) {
	
		
		SparkConf config = new SparkConf().setAppName("printingRDD").setMaster("local[2]");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(config);

		List<String> listOfNumbers = new ArrayList<>();
		listOfNumbers.add("LOGGER Test 15May");
		listOfNumbers.add("ERROR Test 20May");
		listOfNumbers.add("LOGGER Test ");
		listOfNumbers.add(" ");
		listOfNumbers.add("WARNING Server slowness");
		listOfNumbers.add("WARNING Server slowness");
		listOfNumbers.add("FATAL Server slowness");
		//Removing all data whose value is LOGGER
		
		JavaPairRDD<String, Integer> errorValueRDD= sc.parallelize(listOfNumbers).
		flatMap(eachValue->Arrays.asList(eachValue.split(" "))
				.iterator())
					.filter(validValue->  !validValue.equals("LOGGER"))
					.mapToPair(eachString->new Tuple2<String,Integer>(eachString,1))
					.reduceByKey((a,b)->a+b);
		
		errorValueRDD.foreach((eachTuple)->System.out.println(eachTuple._1 +"\tCount\t"+eachTuple._2));
	}

}
