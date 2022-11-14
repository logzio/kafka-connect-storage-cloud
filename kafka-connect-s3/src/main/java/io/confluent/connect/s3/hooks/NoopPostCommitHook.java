/*
 * Add here
 */

package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;

import java.util.List;
import java.util.Map;

public class NoopPostCommitHook implements PostCommitHook {

  @Override
  public void init(S3SinkConnectorConfig config, Map<String, String> additionalParams) {

  }

  @Override
  public void put(List<String> s3ObjectPath) {

  }

  @Override
  public void flush() {

  }

  @Override
  public void close() {

  }
}
