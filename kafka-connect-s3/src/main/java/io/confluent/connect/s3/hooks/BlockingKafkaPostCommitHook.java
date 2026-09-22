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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.errors.RetriableException;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BlockingKafkaPostCommitHook implements PostCommitHook {

  private static final Logger log = LoggerFactory.getLogger(BlockingKafkaPostCommitHook.class);
  private static final DateTimeFormatter timeFormatter =
          DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
  private static final Duration PRODUCER_CLOSE_TIMEOUT = Duration.ofSeconds(30);
  private Pattern pattern;
  private String kafkaTopic;
  private S3SinkConnectorConfig config;
  private KafkaProducer<String, String> kafkaProducer;

  @Override
  public void init(S3SinkConnectorConfig config) {
    String topicsDir = config.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    pattern = Pattern.compile(topicsDir + "/(\\d+)/");
    kafkaTopic = config.getPostCommitKafkaTopic();
    this.config = config;
    kafkaProducer = newKafkaPostCommitProducer(config);
    log.info("BlockingKafkaPostCommitHook initialized successfully");
  }

  @Override
  public void put(List<String> s3ObjectPaths, List<Long> s3ObjectToBaseRecordTimestamp) {
    try {
      ensureProducer();
      List<Future<RecordMetadata>> sent = new ArrayList<>();
      for (int i = 0; i < s3ObjectPaths.size(); i++) {
        String s3ObjectPath = s3ObjectPaths.get(i);
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("accountId", getAccountId(s3ObjectPath).getBytes()));
        headers.add(new RecordHeader("fileTimestamp", getLocalDateTime(s3ObjectPath,
                s3ObjectToBaseRecordTimestamp.get(i)).getBytes()));
        headers.add(new RecordHeader("pathHash",
                getPathHash(s3ObjectPath).getBytes()));
        sent.add(kafkaProducer.send(new ProducerRecord<>(kafkaTopic,
                null, null, null, s3ObjectPath, headers)));
      }

      // Block until every notification is acked before returning, so a failure re-consumes and
      // re-notifies (at-least-once). Idempotent acks=all producer; no Kafka transaction.
      for (Future<RecordMetadata> future : sent) {
        future.get();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      discardProducer();
      throw new RetriableException(e);
    } catch (ExecutionException | KafkaException e) {
      log.error("Failed to produce post-commit notifications, discarding producer and retrying", e);
      discardProducer();
      throw new RetriableException(e);
    }
  }

  private void ensureProducer() {
    if (kafkaProducer == null) {
      // Recreate lazily so a failure here surfaces as a (retriable) ConnectException from the
      // producer factory and is retried by Connect, rather than being thrown from the error path.
      kafkaProducer = newKafkaPostCommitProducer(config);
    }
  }

  private void discardProducer() {
    if (kafkaProducer == null) {
      return;
    }
    try {
      kafkaProducer.close(PRODUCER_CLOSE_TIMEOUT);
    } catch (Exception e) {
      log.warn("Failed to close producer while discarding it", e);
    }
    kafkaProducer = null;
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
    if (kafkaProducer == null) {
      return;
    }
    try {
      kafkaProducer.close(PRODUCER_CLOSE_TIMEOUT);
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
    props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");
    props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

    return new KafkaProducer<>(props);
  }

}
