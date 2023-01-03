package com.example.sparkpomrob.repositories;




import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Repository;
import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.util.Arrays;

@Repository
public class HotelRepository {

    private SparkSession spark = null;

    public void readDataset() throws IOException {
        if(spark == null) this.initSpark();
        Resource resource = new ClassPathResource("Hotel_Reviews.csv");
        String datasetPath = resource.getFile().getPath();
        Dataset<Row> data = this.spark.read().option("header", "true").
                option("inferSchema", "true").csv(datasetPath);
        data.show();
        System.out.println("------------******---------");
        //Dataset<Row> russia = this.spark.sql("SELECT * FROM HOTEL WHERE HOTEL._c5 = Russia");
        //data.select("_c5");
        //russia.show();
        System.out.println(Arrays.toString(data.columns()));
        System.out.println(data.schema().toString());

        System.out.println("-------******----------");
        data.createOrReplaceTempView("arena");

        Dataset<Row> recensioniArena = this.spark.sql("SELECT * FROM arena WHERE arena.Hotel_Name  = \"Hotel Arena\" ");
        recensioniArena.show();
        System.out.println("-------******----------");

        Dataset<Row> recensioniRusse = this.spark.sql("SELECT * FROM arena WHERE arena.Reviewer_Nationality  = \" Russia \" ");
        recensioniRusse.show();



        //data.filter(col("_c5").).show();
        System.out.println("data read!");
    }

    private void initSpark() {
        this.spark = SparkSession.builder().appName("Spark Test for Big Data")
                .config("spark.master", "local").getOrCreate();
    }


}
