package com.atguigu.gamll0213publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gamll0213publisher.mapper")
public class Gamll0213PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gamll0213PublisherApplication.class, args);
    }

}
