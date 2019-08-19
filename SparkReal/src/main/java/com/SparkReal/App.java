package com.SparkReal;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	SparkConf config=new SparkConf().setAppName("realApplication");
    	Logger.getLogger("org.apache").setLevel(Level.WARN);
		//Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		JavaSparkContext sc=new JavaSparkContext(config);
		System.out.println("Conf is Ready");
		Set<String> setOfBoring=new HashSet<>();
		setOfBoring.add("the");
		setOfBoring.add("on");
		
		System.out.println("setOfBoring"+setOfBoring);
		
		JavaRDD<String> fileInitialRDD=sc.textFile("s3://kundanbucket2//input.txt");
		System.out.println("Got Bucket");
		
		JavaRDD<String> formattedString=fileInitialRDD.map(str->str.replaceAll("[^a-zA-Z]", " "));
		
		//System.out.println(formattedString.take(10));
		
		JavaRDD<String> splittedFlatRDD=formattedString.flatMap(str->Arrays.asList(str.toLowerCase().split(" ")).iterator() );
		
		JavaRDD<String> notBoringRDD=splittedFlatRDD.filter(eachValue->!setOfBoring.contains(eachValue) && eachValue.trim().length()>0 );
		
		JavaPairRDD<String, Integer> mappingNonBoring=notBoringRDD.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String value) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(value, 1);
			}
			
			
			
			
		});
		
		JavaPairRDD<String,Integer> eachReverseMapper=mappingNonBoring.reduceByKey((a,b)->a+b);
		
		
		JavaPairRDD<Integer,String> wordCountByKey=eachReverseMapper.mapToPair(eachTuple->new Tuple2<Integer,String>(eachTuple._2, eachTuple._1));
		
		wordCountByKey=wordCountByKey.sortByKey(false);
		System.out.println(wordCountByKey.take(100));
		
		
		
		
		
		
		
		
		
		
		
		
		
	
	
		
		
		
		
		
    }
}
