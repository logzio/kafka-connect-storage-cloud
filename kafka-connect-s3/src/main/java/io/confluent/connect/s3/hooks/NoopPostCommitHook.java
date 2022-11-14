/*
 * Add here
 */

package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Set;

public class NoopPostCommitHook implements PostCommitHook {

  @Override
  public void init(S3SinkConnectorConfig config, SinkTaskContext context) {

  }

  @Override
  public void put(Set<String> s3ObjectPaths) {

  }

  @Override
  public void close() {

  }
}
