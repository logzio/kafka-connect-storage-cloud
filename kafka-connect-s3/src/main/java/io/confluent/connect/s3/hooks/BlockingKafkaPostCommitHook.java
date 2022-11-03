package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.errors.RetriableException;;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BlockingKafkaPostCommitHook implements PostCommitHook {

    private static final Logger log = LoggerFactory.getLogger(BlockingKafkaPostCommitHook.class);
    private String kafkaTopic;
    private KafkaProducer<String, String> kafkaProducer;

    public void init(S3SinkConnectorConfig config) {
        kafkaTopic = config.getPostCommitKafkaTopic();
        kafkaProducer = newKafkaPostCommitProducer(config);
    }

    public void execute(String s3ObjectPath) {
        try {
            Future<RecordMetadata> f = kafkaProducer.send(new ProducerRecord<>(kafkaTopic, s3ObjectPath));
            f.get(15, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Failed to produce to kafka", e);
            throw new RetriableException(e);
        }
    }

    public void close() {
        kafkaProducer.close();
    }

    private KafkaProducer<String, String> newKafkaPostCommitProducer(S3SinkConnectorConfig config) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getPostCommitKafkaBootstrapServers());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "internal-archiver-producer");

        return new KafkaProducer<>(props);
    }

}
