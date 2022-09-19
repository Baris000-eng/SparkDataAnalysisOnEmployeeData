package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class ActionApplication {
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local","Action Application");
        JavaRDD<String> data = javaSparkContext.textFile("data.csv");
        System.out.println("Data count is: "+ data.count()); //for finding total data count
        System.out.println("The first data is: "+ data.first()); //for finding first data
        System.out.println(data.take(3)); //will return first 3 of the entire data.
        //the parameter passed inside to the take() function will determine how many
        //data will be returned from the beginning of the entire data.

        data.saveAsTextFile("w.txt"); //it will save the data to the file named as w.txt.
        data.foreach(new VoidFunction<String>() {
            @Override
            public void call(String string) throws Exception {
                System.out.println(string); //prints each data.
            }
        });


    }
}
