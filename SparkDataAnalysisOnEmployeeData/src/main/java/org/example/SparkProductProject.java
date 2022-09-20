package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


import javax.xml.crypto.Data;

public class SparkProductProject {
    public static void main(String[] args) {
         System.setProperty("hadoop.home.dir","/Users/barissss/Desktop/hadoop-common-2.2.0-bin-master");
        StructType schema = new StructType()
                .add("firstName", DataTypes.StringType)
                .add("lastName", DataTypes.StringType)
                .add("email",DataTypes.StringType)
                .add("country", DataTypes.StringType)
                .add("price", DataTypes.DoubleType)
                .add("product",DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder().master("local").appName("SparkProductProject").getOrCreate();

        Dataset<Row> rawDataset = sparkSession.read().schema(schema).option("multiline",true).json("product.json");

        rawDataset.show();

        Dataset<Row>countryPriceDS = rawDataset.groupBy("country").sum("price");
        countryPriceDS.show();

        Dataset<Row>countryAndProductPriceDS = rawDataset.groupBy("country","product").sum("price");
        countryAndProductPriceDS.show();

        Dataset<Row>countDS = rawDataset.groupBy("country","product").count();

        System.out.println("Sorted");
        countDS.sort(functions.desc("count")).show();
        countDS.show();

        Dataset<Row>countPriceDS = rawDataset.groupBy("country").avg("price");
        countPriceDS.show();


         //globalTempView = cluster üzerinde oluşturuyor. sessiondan bağımsız.
        //createOrReplaceTempView = session bazlı, view sadece sana görünür.
        rawDataset.createOrReplaceTempView("product");
        Dataset<Row> sqlDataSet = sparkSession.sql("select * from product");
        sqlDataSet.show();
        
        Dataset<Row>sqlSecondDataSet = sparkSession.sql("select firstName,lastName FROM product");
        sqlSecondDataSet.show();


        
        
       





        




    }
}
