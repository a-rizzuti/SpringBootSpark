package com.example.sparkpomrob.repositories;




import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Repository;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.Arrays;

@Repository
public class HotelRepository {

    private SparkSession spark = null;


    public void exampleQuery(Dataset<Row> data){
        System.out.println("------------******---------");

        System.out.println(Arrays.toString(data.columns()));
        System.out.println(data.schema().toString());

        System.out.println("-------******----------");
        data.createOrReplaceTempView("arena");

        Dataset<Row> recensioniArena = this.spark.sql("SELECT * FROM arena WHERE arena.Hotel_Name  = \"Hotel Arena\" ");
        recensioniArena.show();
        System.out.println("-------******----------");


        Dataset<Row> recensioniRusse = this.spark.sql("SELECT * FROM arena WHERE arena.Reviewer_Nationality  = \" Russia \" ");
        recensioniRusse.show();
    }

    public Dataset<Row> readDataset() throws IOException {

        if(spark == null) this.initSpark();
        Resource resource = new ClassPathResource("Hotel_Reviews.csv");
        String datasetPath = resource.getFile().getPath();

        Dataset<Row> data = this.spark.read().option("header", "true").
                option("inferSchema", "true").csv(datasetPath);
        //data.show();
        return data;
    }

    public Dataset<Row> getByName(String nome) throws IOException {
        System.out.println("Nome richiesto: "+ nome);
        Dataset<Row> ret = this.readDataset();
        ret.createOrReplaceTempView("recensioni");

        return this.spark.sql("SELECT * FROM recensioni WHERE recensioni.Hotel_Name LIKE \"%"+ nome + "%\"");

    }
    private void initSpark() {
        this.spark = SparkSession.builder().appName("Spark Test for Big Data")
                .config("spark.master", "local").getOrCreate();
    }


}
