/*
 * Add here
 */

package io.confluent.connect.s3.hooks;

import com.amazonaws.util.Md5Utils;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.errors.RetriableException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class BlockingKafkaPostCommitHook implements PostCommitHook {

  private static final Logger log = LoggerFactory.getLogger(BlockingKafkaPostCommitHook.class);
  private static final DateTimeFormatter timeFormatter =
          DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
  private Pattern pattern;
  private String kafkaTopic;
  private KafkaProducer<String, String> kafkaProducer;

  @Override
  public void init(S3SinkConnectorConfig config) {
    String topicsDir = config.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    pattern = Pattern.compile(topicsDir + "/(\\d+)/");
    kafkaTopic = config.getPostCommitKafkaTopic();
    kafkaProducer = newKafkaPostCommitProducer(config);
    log.info("BlockingKafkaPostCommitHook initialized successfully");
  }

  @Override
  public void put(List<String> s3ObjectPaths, List<Long> s3ObjectToBaseRecordTimestamp) {
    try {
      kafkaProducer.beginTransaction();
      log.info("Transaction began");

      IntStream.range(0, s3ObjectPaths.size()).forEach(i -> {
        List<Header> headers = new ArrayList<>();
        String s3ObjectPath = s3ObjectPaths.get(i);
        headers.add(new RecordHeader("accountId", getAccountId(s3ObjectPath).getBytes()));
        headers.add(new RecordHeader("fileTimestamp", getLocalDateTime(s3ObjectPath,
                s3ObjectToBaseRecordTimestamp.get(i)).getBytes()));
        headers.add(new RecordHeader("pathHash",
                getPathHash(s3ObjectPath).getBytes()));
        kafkaProducer.send(new ProducerRecord<>(kafkaTopic,
                null, null, null, s3ObjectPath, headers));
      });

      kafkaProducer.commitTransaction();
      log.info("Transaction committed");
    } catch (ProducerFencedException | AuthorizationException | UnsupportedVersionException
             | IllegalStateException | OutOfOrderSequenceException e) {
      log.error("Failed to begin transaction with unrecoverable exception, closing producer", e);
      throw new ConnectException(e);
    } catch (KafkaException e) {
      log.error("Failed to produce to kafka, aborting transaction and will try again later", e);
      kafkaProducer.abortTransaction();
      throw new RetriableException(e);
    }
  }

  private String getLocalDateTime(String s3ObjectPath, Long baseRecordTimestamp) {
    if (baseRecordTimestamp == null) {
      return null;
    }
    LocalDateTime localDateTime = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(baseRecordTimestamp), ZoneOffset.UTC);

    String formattedTimestamp = localDateTime.format(timeFormatter);
    log.debug("Object: {} has base record timestamp of: {}",
            s3ObjectPath, formattedTimestamp);
    return formattedTimestamp;
  }

  private String getPathHash(String s3ObjectPath) {
    return Md5Utils.md5AsBase64(s3ObjectPath.getBytes()).substring(0, 16)
            // Escape the base64 + and / to safe URL characters
            .replace("+", "A")
            .replace("/", "B");
  }

  private String getAccountId(String s3ObjectPath) {
    Matcher matcher = pattern.matcher(s3ObjectPath);
    if (matcher.find()) {
      return matcher.group(1);
    } else {
      throw new ConnectException("Couldn't create header for accountId");
    }
  }

  @Override
  public void close() {
    try {
      kafkaProducer.close();
    } catch (Exception e) {
      log.error("Failed to close kafka producer", e);
    }
  }

  private KafkaProducer<String, String> newKafkaPostCommitProducer(S3SinkConnectorConfig config) {
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            config.getPostCommitKafkaBootstrapBrokers());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    String id = "blocking-kafka-producer-" + RandomStringUtils.randomAlphabetic(6);
    props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, id);
    props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, id);
    props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");
    props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
    try {
      log.info("stating to initialize transactions");
      kafkaProducer.initTransactions();
      log.info("Transactions initialized");
    } catch (Exception e) {
      log.error("Failed to initiate transaction context", e);
      throw new ConnectException(e);
    }
    return kafkaProducer;
  }

}
