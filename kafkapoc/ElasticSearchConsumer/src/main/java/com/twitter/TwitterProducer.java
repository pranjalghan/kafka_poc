package com.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger= LoggerFactory.getLogger(TwitterProducer.class.getName());
    String consumerKey="tiZNEvkdMWo4DgD4E9IgXzj10";
    String consumerSecret="qPv7qdUKKIvwKUd2WRJBMezdOMYsSoqd2YaX4Oj6NGpABl5Yxt";
    String token="1243483298711527424-I66qHAjyLnfUK769yKa1ZaFBl2TX46";
    String Secret="zcKhOSXHsqjKemIU5omb5BhPXlrhHZ0xHK30Bv9Xw4Mkl";
    List<String> terms = Lists.newArrayList("usa","politics","sports","cricket");
    public TwitterProducer(){
    }
    public static void main(String[] args) {
        new TwitterProducer().run();
    }
    public void run(){
        logger.info("Setup");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        //create a twitter client
        Client client=createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();
        //create a kafka producer
        KafkaProducer<String,String> producer=createKafkaProducer();

        //loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }if(msg!=null){
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets",null,msg),new Callback(){
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.error("something bad happened",e);
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }
    //Create a kafka producer
    public KafkaProducer<String, String> createKafkaProducer() {
//        Create the  producer properties
        Properties properties=new Properties();
        String bootstrapServer="127.0.0.1:9092";
        //NOW TO SEE WHICH PROPERTIES TO ADD ->GO TO KAFKA DOCUMENTATION
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //create a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        //high throughput  producer for good latency
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));//32Kb batch size
        //create the producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        return producer;
    }
    public  Client createTwitterClient(BlockingQueue<String> msgQueue){
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up track terms
        hosebirdEndpoint.trackTerms(terms);
        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, Secret);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        // optional: use this if you want to process client events
        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }
}
