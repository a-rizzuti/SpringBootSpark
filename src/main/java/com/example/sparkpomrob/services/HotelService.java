package com.example.sparkpomrob.services;
import com.example.sparkpomrob.repositories.HotelRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

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

}
