package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {

        JavaSparkContext javaSparkContext = new JavaSparkContext("local","Spark Project");

        //Data Load
        JavaRDD<String>data = javaSparkContext.textFile("data.csv");
        System.out.println("The data count is: "+data.count());
        System.out.println("The first data is: "+data.first());

        List<String> list = Arrays.asList("big data","elastic search", "first", "spark", "hadoop");
        JavaRDD<String>myData = javaSparkContext.parallelize(list);
        System.out.println("Total data count: "+myData.count());
        System.out.println("First data: "+myData.first());
    }



   /* parallelize() method is the SparkContext's parallelize method to create a parallelized collection.
   This allows Spark to distribute the data across multiple nodes, instead of depending on a single node to process the data.*/


}
