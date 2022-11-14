/*
 * Add here
 */

package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An interface represent an action to be performed after a file is commited to S3.
 */
public interface PostCommitHook {

  void init(S3SinkConnectorConfig config, Map<String, String> additionalParams);

  /**
   * @param s3ObjectPath uploaded s3 object path
   */
  default void put(String s3ObjectPath) {
    put(Collections.singletonList(s3ObjectPath));
  }

  void put(List<String> s3ObjectPath);

  /**
   * Persist the file paths to the target system
   */
  void flush();

  void close();
}
