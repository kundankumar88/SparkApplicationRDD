package com.rdd.pairRDD;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class RealUseCaseOfPairToRDDCache {

	public static void main(String[] args) {

		SparkConf config = new SparkConf().setAppName("printingRDD").setMaster("local[2]");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(config);

		List<String> listOfNumbers = new ArrayList<>();
		
		for(int i=0 ;i<1000000;i++)
		{
		listOfNumbers.add("LOGGER:Test");
		listOfNumbers.add("ERROR:  Test Error");
		listOfNumbers.add("LOGGER:Test");
		listOfNumbers.add("FATAL: Server Failure");
		listOfNumbers.add("WARNING: Server slowness");
		listOfNumbers.add("WARNING: Server slowness");
		listOfNumbers.add("FATAL: Server slowness");
		}

		JavaPairRDD<String, Integer> pairRDD = sc.parallelize(listOfNumbers).mapToPair((eachVal) -> {

			String[] arr = eachVal.split(":");

			return new Tuple2<String, Integer>(arr[0], 1);
		}).cache();
		
		//Getting Size of RDD using ReduceBy Key Method

		JavaPairRDD<String, Integer> pairRDDResultValue = pairRDD.reduceByKey((x, y) -> x + y);
		
		pairRDD=pairRDD.cache();

		pairRDDResultValue.foreach(eachVal -> System.out.println(eachVal._1 + "   " + eachVal._2));

		// Group By

		JavaPairRDD<String, String> pairRDDString = sc.parallelize(listOfNumbers).mapToPair((eachVal) -> {

			String[] arr = eachVal.split(":");

			return new Tuple2<String, String>(arr[0], arr[1]);
		});

		// Getting Size Of RDD by each key using group by method: dont use this method in production as it will hamper performance alot

		JavaPairRDD<String, Iterable<String>> groupByRDD = pairRDDString.groupByKey();

		pairRDD.foreach(eachValue -> System.out
				.println("Key     " + eachValue + "    Value  "));
		
		pairRDD.count();
		//pairRDDString.groupByKey().foreach(eachVal->System.out.println(eachVal));
		
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%Done#########################");
		
		Scanner scanner=new Scanner(System.in);
		
		
		scanner.nextLine();
		

		sc.close();

	}

}
