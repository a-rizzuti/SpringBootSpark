package com.example.sparkpomrob.repositories;




import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Repository;

import java.io.IOException;

@Repository
public class HotelRepository {

    private SparkSession spark = null;

    public void readDataset() throws IOException {
        if(spark == null) this.initSpark();
        Resource resource = new ClassPathResource("Hotel_Reviews.csv");
        String datasetPath = resource.getFile().getPath();
        Dataset<Row> data = this.spark.read().csv(datasetPath);
        System.out.println("data read!");
    }

    private void initSpark() {
        this.spark = SparkSession.builder().appName("Spark Test for Big Data")
                .config("spark.master", "local").getOrCreate();
    }


}
