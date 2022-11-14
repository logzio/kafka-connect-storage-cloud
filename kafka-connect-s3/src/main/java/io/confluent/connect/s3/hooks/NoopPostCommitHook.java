/*
 * Add here
 */

package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;

import java.util.Map;
import java.util.Set;

public class NoopPostCommitHook implements PostCommitHook {

  @Override
  public void init(S3SinkConnectorConfig config, Map<String, String> additionalParams) {

  }

  @Override
  public void put(Set<String> s3ObjectPaths) {

  }

  @Override
  public void close() {

  }
}
