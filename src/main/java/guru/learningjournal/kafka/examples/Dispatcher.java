package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.xml.ws.Dispatch;
import java.io.File;
import java.util.Scanner;

public class Dispatcher implements Runnable {
    private static final Logger logger = LogManager.getLogger();
    private String fileName;
    private String topicName;
    KafkaProducer<Integer, String> producer;

    Dispatcher(KafkaProducer<Integer, String> producer, String topic,String fileName){
        this.producer  = producer;
        this.fileName = fileName;
        this.topicName  = topic;
    }

    @Override
    public void  run(){
        logger.info("Start processing : " + fileName);
        int counter = 0;

        File file = new File(fileName);
        try (Scanner scanner = new Scanner(file)) {
            while(scanner.hasNextLine()){
                String line = scanner.nextLine();
                producer.send( new ProducerRecord<Integer, String>(topicName, null, line));
                counter++;
            }
            logger.info("Finished sending " + counter + " messages from " + fileName);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
