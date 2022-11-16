/*
 * Add here
 */

package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.errors.RetriableException;

import java.util.Properties;
import java.util.Set;

public class BlockingKafkaPostCommitHook implements PostCommitHook {

  private static final Logger log = LoggerFactory.getLogger(BlockingKafkaPostCommitHook.class);
  private String kafkaTopic;
  private String transactionalId;
  private KafkaProducer<String, String> kafkaProducer;

  @Override
  public void init(S3SinkConnectorConfig config, SinkTaskContext context) {
    if (kafkaProducer != null) {
      close();
    }
    kafkaTopic = config.getPostCommitKafkaTopic();
    transactionalId = Integer.toString(context.assignment().hashCode());
    kafkaProducer = newKafkaPostCommitProducer(config);
    log.info("BlockingKafkaPostCommitHook initialized successfully");
  }

  @Override
  public void put(Set<String> s3ObjectPaths) {
    try {
      kafkaProducer.beginTransaction();
      log.info("Beginning transaction for: {}", transactionalId);

      for (String s3ObjectPath : s3ObjectPaths) {
        kafkaProducer.send(new ProducerRecord<>(kafkaTopic, s3ObjectPath));
      }

      kafkaProducer.commitTransaction();
      log.info("committed transaction {} successfully", transactionalId);
    } catch (ProducerFencedException | AuthorizationException | UnsupportedVersionException
             | IllegalStateException | OutOfOrderSequenceException e) {
      log.error("Failed to begin transaction with unrecoverable exception, closing producer", e);
      throw new ConnectException(e);
    } catch (KafkaException e) {
      log.error("Failed to begin transaction", e);
      kafkaProducer.abortTransaction();
      throw new RetriableException(e);
    }
  }

  @Override
  public void close() {
    if (kafkaProducer != null) {
      try {
        kafkaProducer.close();
      } catch (Exception e) {
        log.error("Failed to close kafka producer", e);
      }
      kafkaProducer = null;
    }
  }

  private KafkaProducer<String, String> newKafkaPostCommitProducer(S3SinkConnectorConfig config) {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            config.getPostCommitKafkaBootstrapBrokers());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "blocking-kafka-producer-"
            + transactionalId);
    props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");
    props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
    try {
      kafkaProducer.initTransactions();
    } catch (Exception e) {
      log.error("Failed to initiate transaction context", e);
      throw e;
    }
    return kafkaProducer;
  }

}
