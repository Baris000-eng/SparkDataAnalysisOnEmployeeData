package org.example;

import org.apache.hadoop.shaded.com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.example.model.Person;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class MapTransformation {
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local", "Map Transformation Spark");
        JavaRDD<String> data = javaSparkContext.textFile("data.csv");
        System.out.println(data.count()); //for displaying the data count inside the "data.csv" file.


        //distinct
        JavaRDD<String>distinctData = data.distinct(); //gets the distinct elements an stores the distinct elements in a java rdd.
        System.out.println(distinctData.count());


        //flatMap her virgülle ayrılan veriyi ayrı veri olarak sayar.
        JavaRDD<String> stringJavaRDD = data.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        });

        System.out.println(stringJavaRDD.count());

        //map function usage
        JavaRDD<Person> loadPerson = data.map(new Function<String, Person>() {
            public Person call(String line) throws Exception {
                String[] data = line.split(",");
                Person person = new Person();
                person.setFirstName(data[0]);
                person.setLastName(data[1]);
                person.setEmail(data[2]);
                person.setGender(data[3]);
                person.setCountry(data[4]);
                int age = Integer.parseInt(data[5]);
                person.setAge(age);
                System.out.println(person.getFirstName());
                System.out.print("First Name: " + person.getFirstName());
                System.out.print("Last Name: " + person.getLastName());
                System.out.print("Email: " + person.getEmail());
                System.out.print("Gender: " + person.getGender());
                System.out.print("Country: " + person.getCountry());
                return person;

            }
        });

        System.out.println(loadPerson.count()); //Action happened on loadPerson data. Lazy evaluation example.

        loadPerson.saveAsTextFile("file.txt"); //Another possible action on loadPerson data.

        JavaPairRDD<String,String>pairRDD = loadPerson.mapToPair(new PairFunction<Person,String,String>(){

            @Override
            public Tuple2<String, String> call(Person person) throws Exception {
                return new Tuple2<String,String>(person.getEmail(),person.getCountry());
            }
        });

        pairRDD.foreach(new VoidFunction<Tuple2<String,String>>() {

            @Override
            public void call(Tuple2<String, String> data) throws Exception {
                System.out.println("Key: "+data._1+", Value: "+data._2);
            }
        });


        JavaPairRDD<String,Person>personRDD = loadPerson.mapToPair(new PairFunction<Person,String,Person>(){

            @Override
            public Tuple2<String, Person> call(Person person) throws Exception {
                return new Tuple2<String,Person>(person.getCountry(),person);
            }
        });

        JavaPairRDD<String,Iterable<Person>> groupedData = personRDD.groupByKey();
        groupedData.foreach(new VoidFunction<Tuple2<String,Iterable<Person>>>(){

            @Override
            public void call(Tuple2<String, Iterable<Person>> data) throws Exception {
                System.out.println("Key: "+data._1+", Value: "+data._2+"");
            }
        });

        groupedData.foreach(new VoidFunction<Tuple2<String,Iterable<Person>>>(){
            @Override
            public void call(Tuple2<String, Iterable<Person>> data) throws Exception {
                System.out.println("Key: "+data._1+", Value: "+data._2+"");
            }
        });


//printing the name and surname of each data with foreach function
        loadPerson.foreach(new VoidFunction<Person>() {
            public void call(Person person) throws Exception {
                System.out.println("Name: " + person.getFirstName() + ", Surname: " + person.getLastName());
            }
        });


        JavaRDD<Person> personFromUSA = loadPerson.filter(new Function<Person,Boolean>(){
            public Boolean call(Person person) throws Exception {
              return person.getGender().contentEquals("Female") && person.getCountry().contentEquals("USA");
               /*return person.getAge() > 35; */
            }
        });

        System.out.println("The number of people from USA: "+personFromUSA.count());
        personFromUSA.foreach(new VoidFunction<Person>(){
            public void call(Person person) throws Exception {
                System.out.println("Age: "+person.getAge()+", First Name: "+person.getFirstName()+", Last Name: "+person.getLastName()+" , Country: "+person.getCountry()+", Gender: "+person.getGender()+"");
            }
        });










    }
}



