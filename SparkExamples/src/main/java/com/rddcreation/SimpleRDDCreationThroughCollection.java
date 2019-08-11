package com.rddcreation;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleRDDCreationThroughCollection {
	
	
	
	
	public  void simpleRddCreation(List<Integer> listOfNumbers, JavaSparkContext sc)
	{
		
		//One important parameter for parallel collections is the number of partitions to cut the dataset into. Spark will run one task for each partition of the cluster.
		//Typically you want 2-4 partitions for each CPU in your cluster. Normally, Spark tries to set the number of partitions automatically based on your cluster. However,
		//you can also set it manually by passing it as a second parameter to parallelize (e.g. sc.parallelize(data, 10)). Note: some places 
		//in the code use the term slices (a synonym for partitions) to maintain backward compatibility.
		
		
		JavaRDD<Integer > createdRDD=sc.parallelize(listOfNumbers);  //JavaRDD is just a wrapper over Scala RDD 
		System.out.println("RDD Created");
		
		
		
	}
	
	

	public static void main(String[] args) {
		
		SparkConf conf=new SparkConf().setAppName("SimpleRDDCreationThroughCollection").setMaster("local[*]");
		Logger.getLogger("org.apache").setLevel(Level.WARN);  //Just to avoid unnecessary  Logger
		
		
		//The first thing a Spark program must do is to create a JavaSparkContext object,
				//which tells Spark how to access a cluster. To create a SparkContext you first need to build a SparkConf object that contains information about your application.
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> listOfNumbers =new ArrayList<Integer>();
		listOfNumbers.add(10);
		listOfNumbers.add(20);
		listOfNumbers.add(30);
		listOfNumbers.add(6);
		listOfNumbers.add(1);
		SimpleRDDCreationThroughCollection sparkObj=new SimpleRDDCreationThroughCollection();
		
		sparkObj.simpleRddCreation(listOfNumbers,sc);
		
		
		
		
		
		
		
		
		
		
		
		
		
	
		
		
		
	}

}
