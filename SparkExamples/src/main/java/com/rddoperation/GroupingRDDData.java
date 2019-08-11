package com.rddoperation;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.rddoperation.util.SquareRootHolderClass;

public class GroupingRDDData {
	
	//This Program is created to accomodate two /more values in RDD like HashMap like Key Value Pairs
	//Also This program should be done with Tuple 2 but its just created so that we know that there is an alternate for tuples
	

	public static void main(String[] args) {
		
		SparkConf config=new SparkConf().setAppName("printingRDD").setMaster("local[2]");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		JavaSparkContext sc=new JavaSparkContext(config);
		
		List<Integer> listOfNumbers =new ArrayList<Integer>();
		listOfNumbers.add(10);
		listOfNumbers.add(20);
		listOfNumbers.add(30);
		listOfNumbers.add(6);
		listOfNumbers.add(1);
		
		JavaRDD<Integer> myRDD=sc.parallelize(listOfNumbers);
		
		SquareRootHolderClass starrRootObj=new SquareRootHolderClass();
		
		JavaRDD<SquareRootHolderClass> squareRootHolderRDD=myRDD.map(eachListValue-> new SquareRootHolderClass(new Double(eachListValue)));
		
		//Printing Each SquareRootHolder RDD
		squareRootHolderRDD.foreach(eachObject->System.out.println("Number"+eachObject.getMainNumber()+" Square Roots"+eachObject.getSquareRoot()));
		
		
		sc.close();
		
		
		

	}

}
