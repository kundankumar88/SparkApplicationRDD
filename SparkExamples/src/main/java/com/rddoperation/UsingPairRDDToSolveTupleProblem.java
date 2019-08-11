package com.rddoperation;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

//This is similar to UsingTupleToGroupNumberSquareRoot just one difference that the same problems can be resolved through PairRDD in an 
//Efficient Way ..Also , Pair RDD has some important extra method like reduceBy Key that is very popular for grouping Key Pair
//On the basis of keys


public class UsingPairRDDToSolveTupleProblem {
	
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
		JavaPairRDD<Long,Double> squareRootHolderRDD=myRDD.mapToPair(eachValue->new Tuple2<Long,Double>(eachValue, Math.sqrt(eachValue)));
		squareRootHolderRDD.foreach(eachTuple->System.out.println("Number"+eachTuple._1+" Square Roots"+eachTuple._2));
		
		
		//Count the number Of elements 1st way
		
		System.out.println("Through Normal Count Method"+squareRootHolderRDD.count());
		//Count the number Of elements 2nd way
		
		JavaRDD<Long> countRDD=squareRootHolderRDD.map(eachValue->1L);
		Long countValueThroughReduce=countRDD.reduce((x,y)->x+y);
		System.out.println("countValue Through Reduce is "+countValueThroughReduce);
		
		
		
		sc.close();
		
		
		

	}

}
