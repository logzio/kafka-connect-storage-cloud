# Kafka Connect Connector for S3

*kafka-connect-storage-cloud* is the repository for Confluent's [Kafka Connectors](http://kafka.apache.org/documentation.html#connect)
designed to be used to copy data from Kafka into Amazon S3. 

## Kafka Connect Sink Connector for Amazon Simple Storage Service (S3)

Documentation for this connector can be found [here](http://docs.confluent.io/current/connect/connect-storage-cloud/kafka-connect-s3/docs/index.html).

Blogpost for this connector can be found [here](https://www.confluent.io/blog/apache-kafka-to-amazon-s3-exactly-once).

# Build

Kafka Connect Storage Common modules use a few dependencies which we sometimes use SNAPSHOT versions of. We do this during the development of a new release in order to build and test against new features. If you want to build a development version, you may need to build and install these dependencies to your local Maven repository in order to build the connector:

- **Kafka** - clone https://github.com/confluentinc/kafka/ and build with `./gradlew build -x test` & `./gradlew -PskipSigning=true publishToMavenLocal`
- **Common** - clone https://github.com/confluentinc/common and build with `mvn install -DskipTests`
- **Rest Utils** - clone https://github.com/confluentinc/rest-utils and build with `mvn install -DskipTests`
- **Avro converter** - clone https://github.com/confluentinc/schema-registry and build with `mvn install -DskipTests`

Then, to build Kafka Connect Storage Common modules and make them available to storage sink connectors, do the same on the current repo:
- **Kafka Connect Storage Common** - clone https://github.com/logzio/kafka-connect-storage-common and build with `mvn install -DskipTests -Dcheckstyle.skip=true`

# Packing

In order to publish a docker image for a connector using this repository,
you need to pack the code into a fat jar.
To do this, run the following command:

`mvn clean package -Dcheckstyle.skip=true`

# Publish

You can publish multiple connectors with different configurations, 
In this guide we will take our `internal-archiver` connector as an example.

1. Copy the jar from `kafka-connect-s3/target/kafka-connect-s3-<version>-jar-with-dependencies.jar` to replace current jar in the [docker image](https://github.com/logzio/docker-additional/tree/master/external-images/logzio-internal-archiver).
2. Create a new tag for the docker image with the new version by changing [this](https://github.com/logzio/docker-additional/blob/master/external-images/logzio-internal-archiver/build.bash#L3) version.
3. Push the new tag to the docker registry by running [./build.bash](https://github.com/logzio/docker-additional/blob/master/external-images/logzio-internal-archiver/build.bash#L3).

> [!WARNING]  
> Make sure to update the version in the [build.bash](https://github.com/logzio/docker-additional/blob/master/external-images/logzio-internal-archiver/build.bash#L3) file before running it, because it can override the current image.