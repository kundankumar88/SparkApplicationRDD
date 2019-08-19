package com.rdd.loadingTextFiles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class TextFileLoadings {
	
public static void main(String[] args) {
	
		
		SparkConf config = new SparkConf().setAppName("printingRDD").setMaster("local[*]");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(config);
		JavaRDD<String> inputRDD=sc.textFile("src/main/resources/input.txt",50);
		
		

	/*	List<String> listOfNumbers = new ArrayList<>();
		listOfNumbers.add("LOGGER Test 15May");
		listOfNumbers.add("ERROR Test 20May");
		listOfNumbers.add("LOGGER Test ");
		listOfNumbers.add(" ");
		listOfNumbers.add("WARNING Server slowness");
		listOfNumbers.add("WARNING Server slowness");*/
		
		//Removing all data whose value is LOGGER
		
		ArrayList<String> allValidString =new ArrayList<>();
		allValidString.add("anything");
		allValidString.add("application");
		allValidString.add("spelling");
		allValidString.add("load");
		
		
		
		 JavaPairRDD<String, Integer>inputRDD1=inputRDD.flatMap(eachValue->Arrays.asList(eachValue.split(" "))
				.iterator())
					//.filter(validValue->  allValidString.contains(validValue))
					.mapToPair(eachString->new Tuple2<String,Integer>(eachString,1))
					.reduceByKey((a,b)->a+b);
		 
		 
		
		
		 inputRDD1.mapToPair(eachVal->new Tuple2<Integer,String>(eachVal._2, eachVal._1)).
					sortByKey(false).collect();
		 inputRDD1=	inputRDD1.cache();
				
				
			
				
				
				//Number OfPartitions:
				
				/*System.out.println("Number Of Partitions"+inputRDD.partitions().size());*/
				System.out.println("Number Of Cores"+inputRDD.getNumPartitions());
				
				
				
				
				//
				
			//	inputRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
					
					/**
					 * 
					 */
					/*private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<String> t) throws Exception {
					System.out.println(Iterables.size(Arrays.asList(t)));
						
					}
				});*/
				
				
		
		
	}


}
