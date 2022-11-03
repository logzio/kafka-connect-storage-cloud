package io.confluent.connect.s3.hooks;

import io.confluent.connect.s3.S3SinkConnectorConfig;

/**
 * An interface represent an action to be performed after a file is commited to S3.
 */
public interface PostCommitHook {

    void init(S3SinkConnectorConfig config);

    /**
     * @param s3ObjectPath uploaded s3 object path
     */
    void execute(String s3ObjectPath);

    void close();
}
