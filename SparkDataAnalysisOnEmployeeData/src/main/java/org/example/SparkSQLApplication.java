package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkSQLApplication {
    public static void main(String[] args) {
        StructType schema =
                new StructType().
                add("firstName", DataTypes.StringType).
                add("lastName",DataTypes.StringType).
                add("email", DataTypes.StringType).
                add("gender",DataTypes.StringType).
                add("country",DataTypes.StringType).
                add("age", DataTypes.IntegerType);


        SparkSession sparkSession = SparkSession.builder().master("local").appName("SparkSQLProject").getOrCreate();
        Dataset<Row> rawData = sparkSession.read().option("header",true).schema(schema).csv("data.csv");
        Dataset<Row> selectedDataset = rawData.select("firstName","lastName","email");
        System.out.println("Selected Data Set: ");
        selectedDataset.show();

        Dataset<Row> russiaData = rawData.filter(rawData.col("country").equalTo("Russia"));
        System.out.println("Russia Data Set: ");
        russiaData.show();

        Dataset<Row> age50DS = selectedDataset.filter("age > 50");
        age50DS.show();

        Dataset<Row>sortedAge50DS = selectedDataset.filter("age > 50").sort("age");
        sortedAge50DS.show();


        System.out.println("And data set: ");
        Dataset<Row>andDataSet = rawData.filter(rawData.col("country").equalTo("Russia").and(rawData.col("age").gt(25)));
        andDataSet.show();

        System.out.println("Triple condition data set: ");
        Dataset<Row>tripleConditionSet = rawData.filter(rawData.col("country").equalTo("USA").and(rawData.col("age").gt(10)).and(rawData.col("email").contains("harvard")));
        tripleConditionSet.show();

        System.out.println("Or data set: ");
        Dataset<Row>orCountryDS = rawData.filter(rawData.col("country").equalTo("USA").or(rawData.col("country").equalTo("Uzbekistan")));
        orCountryDS.show();

        System.out.println("Saudi Arabia Or USA Dataset: ");
        Dataset<Row>saudiArabiaOrUSADataset = rawData.filter("country = 'Saudi Arabia' or country = 'USA' ");
        saudiArabiaOrUSADataset.show();


        System.out.println("First Name and Last Name Group By Dataset: ");
        Dataset<Row>firstNameAndLastNameGroupByDataSet = rawData.groupBy("firstName", "lastName").count();
        firstNameAndLastNameGroupByDataSet.show();



        rawData.show();
        rawData.printSchema();

        Dataset<Row>countryGroupDataset = rawData.groupBy("country").count();
        countryGroupDataset.show();

        //Dataset<Row>rds = sparkSession.read().schema(schema).option("multiline",true).json("jsp);
        //rds.show();

        //DataSet<Row> rawData = sparkSession.read().csv("data.csv");
        //DataSet<Row> rawData = sparkSession.read().option("header",true).csv("data.csv"); (if we want to specify the headers explicitly)

        System.out.println("With Column Dataset: ");
        Dataset<Row> withColumnDataSet = rawData.withColumn("firstNameTest",rawData.col("firstName")); //specify edilen columnun kopyasını oluşturmak için
        withColumnDataSet.show();


        rawData.show(); //tüm datayı ekranda gösterir.
        rawData.printSchema(); //kolon isimlerini ve tiplerini getirir.
        //Dataset<Row> selectedData = rawData.select("_c0","_c1"); //sadece ilk columnu ve ikinci columnu getir !

        Dataset<Row> selectedData = rawData.select("firstName","lastName");
        //if we specify headers explicitly, we select some data from entire data by passing the specified column names.
        selectedData.show(); //seçilen datayı göster
       // selectedData.printSchema(); //seçilen datanın tipini ve şemasını getir.

        //A schema is the description of the structure of your data (which together create a Dataset in Spark SQL).


        //Apache Spark veri hangi tipte olursa olsun veri üzerinde herhangi bir işlem yapılmadığı sürece gelen veriyi String olarak okur.




    }
}
