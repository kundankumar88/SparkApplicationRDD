package com.rdd.joins;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

//join(otherDataset, [numPartitions])	When called on datasets of type (K, V) and (K, W), 
//returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are
//supported through leftOuterJoin, rightOuterJoin, and fullOuterJoin.

public class SparkJoins {

	public static void main(String[] args) {
		SparkConf config = new SparkConf().setAppName("printingRDD").setMaster("local[2]");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(config);

		List<Tuple2<Integer,String >> listOfUsers = new ArrayList<>();
		//Creating a dummy Collection /Table Like strcture:
		
		listOfUsers.add(new Tuple2<Integer, String>(1, "Kundan"));
		listOfUsers.add(new Tuple2<Integer, String>(2, "Kumar"));
		listOfUsers.add(new Tuple2<Integer, String>(4, "Ananad"));
		listOfUsers.add(new Tuple2<Integer, String>(7, "Amit"));
		listOfUsers.add(new Tuple2<Integer, String>(5, "Rohan"));
		
		JavaPairRDD<Integer,String> rdd1=sc.parallelizePairs(listOfUsers);
		
		
		
		//Creating second collection name and subject name :
		List<Tuple2<Integer,String >> listOfSubjects = new ArrayList<>();
		listOfSubjects.add(new Tuple2<Integer, String>(1,"Maths"));
		listOfSubjects.add(new Tuple2<Integer, String>(2,"Science"));
		listOfSubjects.add(new Tuple2<Integer, String>(5,"Social Science"));
		//Second RDD for joining to RDD
		JavaPairRDD<Integer,String> rdd2=sc.parallelizePairs(listOfSubjects);
		
		//Inner Join is same as join in spark
		
		JavaPairRDD<Integer, Tuple2<String, String>> finalRDD=rdd2.join(rdd1);
		finalRDD.foreach(eachE->System.out.println(eachE));
		
		System.out.println("Left  Outer Join Example");
		//Left Outer Join operation below  Functional Programming supports optional objects to give more flexible data in case of blank 
		JavaPairRDD<Integer, Tuple2<String, Optional<String>>> leftOuterJoinData = rdd1.leftOuterJoin(rdd2);
		//JavaPairRDD<Integer, Tuple2<String, String>> leftOuterJoinData1 = rdd1.leftOuterJoin(rdd2);
		leftOuterJoinData.foreach(eachData->System.out.println(eachData));
		
		//Right Outer Join 
		JavaPairRDD<Integer, Tuple2<Optional<String>, String>> rightOuterJoinData = rdd1.rightOuterJoin(rdd2);
		System.out.println("Right Outer Join Example");
		rightOuterJoinData.foreach(eachData->System.out.println(eachData));
		
	
		
		
		
		
		
		
		
		
		
		
	}

}
