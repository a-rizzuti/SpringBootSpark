package com.example.sparkpomrob.controllers;


import com.example.sparkpomrob.services.HotelService;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("hotel")
public class HotelController {

    @Autowired
    HotelService service;

    @GetMapping("hello")
    String hello(@RequestParam String nome){
        return "hello "+ nome;
    }

    @GetMapping("test")
    void test(){
        this.service.test();
    }

    @GetMapping("nome")
    Dataset<String> getHotelReview(@RequestParam String nome){
        return this.service.getByHotel(nome);
    }
}
