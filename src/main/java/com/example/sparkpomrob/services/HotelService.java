package com.example.sparkpomrob.services;
import com.example.sparkpomrob.repositories.HotelRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;

@Service
public class HotelService {

    @Autowired
    HotelRepository repository;
    public void test(){
        try {
            this.repository.readDataset();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public Dataset<String> getByHotel(String nome){
        try{
            Dataset<Row> ret = this.repository.getByName(nome);
            ret.show();
            return ret.toJSON();
        } catch( IOException e){
            throw new RuntimeException(e);
        }


    }

}
