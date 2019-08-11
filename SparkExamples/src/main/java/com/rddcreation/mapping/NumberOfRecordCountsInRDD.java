package com.rddcreation.mapping;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class NumberOfRecordCountsInRDD {

	public static void main(String[] args) {
		SparkConf config=new SparkConf().setAppName("mappingApplication").setMaster("local[2]");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		JavaSparkContext sc=new JavaSparkContext(config);
		
		List<Integer> listOfNumbers =new ArrayList<Integer>();
		listOfNumbers.add(10);
		listOfNumbers.add(20);
		listOfNumbers.add(30);
		listOfNumbers.add(6);
		listOfNumbers.add(1);
		
		JavaRDD<Integer> myRDD=sc.parallelize(listOfNumbers);
		
		//Mapper Code
		JavaRDD<Long> countMapRDD=myRDD.map(eachElement->1L);
		
		//Reducer Code 
		Long countValue=countMapRDD.reduce((x,y)->x+y);
		
		System.out.println(countValue);
		
		
		
		

	}

}
