
package org.kaliy.kafka.connect.rss;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.ConnectRestServer;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class StandaloneKafkaConnectTest {
    private static final Logger logger = LoggerFactory.getLogger(StandaloneKafkaConnectTest.class);

    private Thread connectThread;
    private CountDownLatch stopLatch;

    private final String offsetsFilename;
    private final int maxTasks;
    private final String urls;
    private final String bootstrapServer;

    public StandaloneKafkaConnectTest(String offsetsFilename, int maxTasks, String urls, String bootstrapServer) {
        this.offsetsFilename = offsetsFilename;
        this.maxTasks = maxTasks;
        this.urls = urls;
        this.bootstrapServer = bootstrapServer;
    }

    public void start() {
        stopLatch = new CountDownLatch(1);
        connectThread = new Thread(() -> {
            Map<String, String> workerProps = workerProps();

            Plugins plugins = new Plugins(workerProps);
            plugins.compareAndSwapWithDelegatingLoader();

            StandaloneConfig config = new StandaloneConfig(workerProps);

            // Get Kafka cluster id using Connect utils (works on 4.1)
            //final String kafkaClusterId = ConnectUtils.lookupKafkaClusterId(config.originals());
            final String kafkaClusterId = "standalone-cluster";

            // REST server (4.1-era signature: port + RestClient + propsMap)
            int port = 8083;
            RestClient restClient = new RestClient(config);
            ConnectRestServer rest = new ConnectRestServer(port, restClient, Collections.emptyMap());

            try {
                rest.initializeServer();
            } catch (Exception e) {
                logger.error("Failed to initialize Connect REST server", e);
                return;
            }

            URI advertisedUrl = rest.advertisedUrl();
            String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

            ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy =
                    plugins.newPlugin(
                            config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
                            config, ConnectorClientConfigOverridePolicy.class
                    );

            // Key converter (schemas disabled for brevity in tests)
            JsonConverter keyConverter = new JsonConverter();
            keyConverter.configure(
                    Collections.singletonMap("schemas.enable", "false"),
                    /* isKey */ true
            );

            // FileOffsetBackingStore in 4.1 expects a Converter in the constructor
            FileOffsetBackingStore offsets = new FileOffsetBackingStore(keyConverter);

            // Worker: keep argument order for 4.1-era signature
            Worker worker = new Worker(
                    workerId,
                    Time.SYSTEM,
                    plugins,
                    config,
                    offsets,
                    connectorClientConfigOverridePolicy
            );

            Herder herder = new StandaloneHerder(worker, kafkaClusterId, connectorClientConfigOverridePolicy);
            final Connect connect = new Connect(herder, rest);

            logger.info("Kafka Connect standalone worker has been initialized");

            try {
                connect.start();

                // Create connector
                Map<String, String> connectorProps = connectorProps();
                FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>((error, info) -> {
                    if (error != null) {
                        logger.error("Failed to create job", error);
                    } else {
                        logger.info("Created connector {}", info.result().name());
                    }
                });

                herder.putConnectorConfig(
                        connectorProps.get(ConnectorConfig.NAME_CONFIG),
                        connectorProps,
                        false,
                        cb
                );
                cb.get();

                // Stay alive until stopped
                stopLatch.await();

            } catch (Throwable t) {
                logger.error("Stopping after connector error", t);
                try {
                    connect.stop();
                    connect.awaitStop();
                } catch (Exception ignored) { }
            } finally {
                try {
                    connect.stop();
                    connect.awaitStop();
                } catch (Exception e) {
                    logger.warn("Error stopping Connect", e);
                }
                try {
                    rest.stop();
                } catch (Exception e) {
                    logger.warn("Error stopping REST server", e);
                }
                logger.info("Kafka Connect standalone worker has been stopped");
            }
        }, "embedded-connect-thread");

        connectThread.setDaemon(true);
        connectThread.start();
    }

    public void stop() {
        if (stopLatch != null) {
            stopLatch.countDown();
        }
        if (connectThread != null) {
            try {
                connectThread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void deleteOffsetsFile() {
        if (!new File(offsetsFilename).delete()) {
            logger.debug("Offsets file {} did not exist or could not be deleted", offsetsFilename);
        }
    }

    private Map<String, String> workerProps() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("bootstrap.servers", bootstrapServer);

        // Converters
        workerProps.put("key.converter", JsonConverter.class.getName());
        workerProps.put("key.converter.schemas.enable", "true");
        workerProps.put("value.converter", JsonConverter.class.getName());
        workerProps.put("value.converter.schemas.enable", "true");
        workerProps.put("internal.key.converter", JsonConverter.class.getName());
        workerProps.put("internal.key.converter.schemas.enable", "true");
        workerProps.put("internal.value.converter", JsonConverter.class.getName());
        workerProps.put("internal.value.converter.schemas.enable", "true");

        // REST (port/host can be overridden by your ConnectRestServer instantiation)
        workerProps.put("rest.port", "8086");
        workerProps.put("rest.host.name", "127.0.0.1");

        // Offsets
        workerProps.put("offset.storage.file.filename", offsetsFilename);
        workerProps.put("offset.flush.interval.ms", "10000");
        return workerProps;
    }

    private Map<String, String> connectorProps() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "RssSourceConnectorDemo");
        props.put("tasks.max", Integer.toString(maxTasks));
        props.put("sleep.seconds", "2");
        props.put("connector.class", "org.kaliy.kafka.connect.rss.RssSourceConnector");
        props.put("rss.urls", urls);
        props.put("topic", "test_topic");
        return props;
    }
}
