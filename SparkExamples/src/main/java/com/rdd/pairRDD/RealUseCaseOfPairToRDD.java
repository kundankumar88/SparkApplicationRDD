package com.rdd.pairRDD;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class RealUseCaseOfPairToRDD {

	public static void main(String[] args) {
		
		SparkConf config=new SparkConf().setAppName("printingRDD").setMaster("local[2]");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		JavaSparkContext sc=new JavaSparkContext(config);
		
		List<String> listOfNumbers =new ArrayList<>();
		listOfNumbers.add("LOGGER:Test");
		listOfNumbers.add("ERROR:  Test Error");
		listOfNumbers.add("LOGGER:Test");
		listOfNumbers.add("FATAL: Server Failure");
		listOfNumbers.add("WARNING: Server slowness");
		listOfNumbers.add("WARNING: Server slowness");
		listOfNumbers.add("FATAL: Server slowness");
		
	JavaPairRDD<String,Integer> pairRDD=sc.parallelize(listOfNumbers).mapToPair((eachVal)->{
			
			String [] arr=eachVal.split(":");
			
			return  new Tuple2<String, Integer>(arr[0], 1);
		}
	);
	
	JavaPairRDD<String, Integer> pairRDDResultValue=pairRDD.reduceByKey((x,y)->x+y);
	
	
	
	pairRDDResultValue.foreach(eachVal->System.out.println(eachVal._1 +"   "+eachVal._2));
	
	
	
	
	
		
		
	}
	
	
		
		
		
		
		
		
		
	

	

}
