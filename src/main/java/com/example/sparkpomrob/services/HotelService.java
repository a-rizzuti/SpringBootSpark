package com.example.sparkpomrob.services;
import com.example.sparkpomrob.dto.RowDTO;
import com.example.sparkpomrob.repositories.HotelRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;


@Service
public class HotelService {

    @Autowired
    HotelRepository repository;
    public void test(){
        try {
            this.repository.exampleQuery();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public List<RowDTO> getByHotel(String nome, int limit)  {
        try{

            Dataset<Row> data = this.repository.getByName(nome,limit);
            // stream -> introdotto con le lambda ed è consigliato per le liste grandi
            // map -> fa svolgere operazioni sui singoli elementa dalla lista
            // collect -> si usa sugli array/dataset/hashmap e serve per convertire la lista nel tipo di lista voluta
            return data.collectAsList().stream().map( d-> RowDTO.convertFromRow(d)).collect(Collectors.toList());

        } catch( IOException e){
            throw new RuntimeException(e);
        }


    }

}
