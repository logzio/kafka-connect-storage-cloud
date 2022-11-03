package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;

public class NoopPostCommitHook implements PostCommitHook {

    @Override
    public void init(S3SinkConnectorConfig config) {

    }

    @Override
    public void execute(String s3ObjectPath) {

    }

    @Override
    public void close() {

    }
}
