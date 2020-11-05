package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DispatchCreator {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args){
        Properties props = new Properties();
        try {
            InputStream inputstream = new FileInputStream(AppConfigs.kafkaConfigFileLocation);
            props.load(inputstream);
            props.put(ProducerConfig.CLIENT_ID_CONFIG,AppConfigs.applicationID);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KafkaProducer <Integer, String> producer = new KafkaProducer<Integer, String>(props);

        Thread []  dispatchers = new Thread [AppConfigs.eventFiles.length];
        logger.info("Starting dispatchers ");
        for(int i=0;i<AppConfigs.eventFiles.length;i++){
            dispatchers[i] = new Thread(new Dispatcher(producer, AppConfigs.topicName, AppConfigs.eventFiles[i]));
            dispatchers[i].start();
        }

       try{
            for(Thread t : dispatchers){
                t.join();
            }
        } catch (InterruptedException e){
           logger.error("Main thread interrupted");
       } finally{
           producer.close();
           logger.info("Finished dispatcherCreator");
       }
    }
}
