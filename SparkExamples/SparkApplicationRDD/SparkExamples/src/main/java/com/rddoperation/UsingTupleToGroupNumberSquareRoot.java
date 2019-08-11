package com.rddoperation;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

//This class will implement GroupingRDDData class implemenattion in better way using tuples

//Tuple is a Scala data type that is use to store data 

public class UsingTupleToGroupNumberSquareRoot {
	
public static void main(String[] args) {
		
		SparkConf config=new SparkConf().setAppName("printingRDD").setMaster("local[2]");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		JavaSparkContext sc=new JavaSparkContext(config);
		
		List<Long> listOfNumbers =new ArrayList<>();
		listOfNumbers.add(10L);
		listOfNumbers.add(20L);
		listOfNumbers.add(30L);
		listOfNumbers.add(6L);
		listOfNumbers.add(1L);
		listOfNumbers.add(60L);
		listOfNumbers.add(13L);
		
		JavaRDD<Long> myRDD=sc.parallelize(listOfNumbers);
		
		
		
		JavaRDD<Tuple2<Long,Double>> squareRootHolderRDD=myRDD.map(eachListValue-> new Tuple2<Long,Double>(eachListValue,Math.sqrt(eachListValue)));
		
		
		squareRootHolderRDD.foreach(eachTuple->System.out.println("Number"+eachTuple._1+" Square Roots"+eachTuple._2));
		
		
		
		
		
		
		sc.close();
		
		
		

	}

}
