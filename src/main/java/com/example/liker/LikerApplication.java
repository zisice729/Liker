
package com.example.liker;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 点赞系统启动类
 */
@SpringBootApplication
@EnableScheduling
@EnableKafka
@EnableCaching
@MapperScan("com.example.liker.mapper")
public class LikerApplication {

    public static void main(String[] args) {
        SpringApplication.run(LikerApplication.class, args);
    }
}
