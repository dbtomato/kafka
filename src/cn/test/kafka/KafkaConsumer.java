package cn.test.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.HashMap;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;

public class KafkaConsumer implements Runnable{
	private ConsumerConfig consumerConfig;
	private static String topic="gj";
	Properties props;
	final int threadNum=3;
	
	
	public KafkaConsumer() {
     props=new Properties();
     props.put("zookeeper.connect","192.168.60.81:2181,192.168.60.50:2181,192.168.60.82:2181");
     props.put("group.id", "blog");  
     props.put("zookeeper.session.timeout.ms", "400");
     props.put("zookeeper.sync.time.ms", "200");
     props.put("auto.commit.interval.ms", "1000"); 
     props.put("auto.offset.reset", "smallest");
     consumerConfig = new ConsumerConfig(props);
	}


	@Override
	public void run() {
		Map<String,Integer> topicCountMap=new HashMap<String,Integer>();
	    topicCountMap.put(topic,threadNum);
	    ConsumerConfig consumerConfig=new ConsumerConfig(props);
	    kafka.javaapi.consumer.ConsumerConnector consumer= Consumer.createJavaConsumerConnector(consumerConfig);
		Map<String,List<KafkaStream<byte[], byte[]>>>consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[],byte[]>>streams=consumerMap.get(topic);
		ExecutorService executor=Executors.newFixedThreadPool(threadNum);
		for(final KafkaStream stream:streams){
			executor.submit(new KafkaConsumerThread(stream));
		}
	}
		
}
