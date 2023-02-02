package com.kafkaspring.service;

import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import com.kafkaspring.entities.PageEvents;

@Service
public class PageEventsService {
	@Bean
	public Consumer<PageEvents> pageEventConsumer(){
		return (input)->{
			System.out.println("**********************");
			System.out.println(input.toString());
			System.out.println("*********");
		};
	}
	
	@Bean
	public Supplier<PageEvents> supplier(){
		return ()-> new PageEvents(Math.random()>0.5?"P1":"P2", Math.random()>0.5?"U1":"U2", new Date(), new Random().nextLong(9000));
	}
	
	@Bean
	public Function<PageEvents, PageEvents> PagefunctionTest(){
		return (input)->{
			input.setName("page event");
			input.setUser("Bmw");
			return input;
		};
	}
	
	@Bean
	public Function<KStream<String, PageEvents>, KStream<String, Long>> kStremFunction(){
		return (input)->{
			return input
					.filter((k,v)->v.getDuration()>100)
					.map((k,v)->new KeyValue<>(v.getName(),0L))
					.groupBy((k,v)->k,Grouped.with(Serdes.String(),Serdes.Long()))
					.count(Materialized.as("page-count"))
					.toStream();
		};
	}
	

}
