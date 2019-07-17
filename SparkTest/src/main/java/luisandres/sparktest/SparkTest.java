/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package luisandres.sparktest;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import org.apache.spark.sql.types.*;
 /**
 *
 * @author lavb0
 */
public class SparkTest {
    public static void main(String[] args) {
        SparkTest sparkProblem = new SparkTest();
        sparkProblem.problem_1();
    }

    public boolean test_core(){
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("accounts.csv");
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);                
        System.out.println("total " + totalLength);
        return true;
    }

    
    public boolean problem_1(){        
        SparkConf conf = new SparkConf().setAppName("spark test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);        
        SparkSession spark = SparkSession.builder().getOrCreate();    
        Dataset<Row> customers = spark.read().json("customer.json");
        Dataset<Row> accounts = spark.read().csv("accounts.csv");  
        customers.createOrReplaceTempView("customers");
        accounts.createOrReplaceTempView("accounts");
        customers.show();
        accounts.show();
        Dataset<Row> problem1 = customers.join(accounts, customers.col("client").equalTo(accounts.col("_c2")));
        problem1.show();
        Dataset<Row> problem2 = problem1.withColumn("_c3", problem1.col("_c3").cast(DataTypes.DoubleType)).select("client","name","_c0","_c3").groupBy("client","name","_c0").sum("_c3");
        problem2.show();
        Dataset<Row> parameters = customers.withColumn("age", customers.col("age").cast(DataTypes.DoubleType)).filter(customers.col("age").$greater$eq(100)).select("age").agg(max("age"),min("age"));
        parameters.show();
        return true;
    }
}
