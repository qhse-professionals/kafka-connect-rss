
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
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Embedded standalone Kafka Connect runner for an RSS connector.
 * Compatible with Kafka/Connect 4.1.
 */
public class StandaloneKafkaConnect {
    private static final Logger logger = LoggerFactory.getLogger(StandaloneKafkaConnect.class);

    private Thread connectThread;
    private CountDownLatch stopLatch;

    private final String offsetsFilename;
    private final int maxTasks;
    private final String urls;
    private final String bootstrapServer;

    public StandaloneKafkaConnect(String offsetsFilename, int maxTasks, String urls, String bootstrapServer) {
        this.offsetsFilename = offsetsFilename;
        this.maxTasks = maxTasks;
        this.urls = urls;
        this.bootstrapServer = bootstrapServer;
    }

    public void start() {
        stopLatch = new CountDownLatch(1);
        connectThread = new Thread(() -> {
            // 1) Worker/Plugin setup
            Map<String, String> workerProps = workerProps();
            Plugins plugins = new Plugins(workerProps);
            plugins.compareAndSwapWithDelegatingLoader();

            StandaloneConfig config = new StandaloneConfig(workerProps);

            // 2) Discover Kafka cluster ID
            final String kafkaClusterId;
            try (Admin admin = Admin.create(config.originals())) {
                kafkaClusterId = admin.describeCluster()
                        .clusterId()
                        .get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("Failed to obtain Kafka cluster ID", e);
                return;
            }

            // 3) REST server
            RestClient restClient = new RestClient(config);
            ConnectRestServer rest = new ConnectRestServer(config, restClient, Collections.emptyMap());
            try {
                rest.initializeServer();
            } catch (Exception e) {
                logger.error("Failed to initialize Connect REST server", e);
                return;
            }

            URI advertisedUrl = rest.advertisedUrl();
            String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

            // 4) Policy for connector client config overrides
            ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy =
                    plugins.newPlugin(
                            config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
                            config,
                            ConnectorClientConfigOverridePolicy.class
                    );

            // 5) Offset store
            FileOffsetBackingStore offsetStore = new FileOffsetBackingStore();
            try {
                offsetStore.configure(config);
                offsetStore.start();
            } catch (Exception e) {
                logger.error("Failed to start FileOffsetBackingStore", e);
                return;
            }

            // 6) Worker
            Worker worker = new Worker(
                    workerId,
                    Time.SYSTEM,
                    plugins,
                    config,
                    offsetStore,
                    connectorClientConfigOverridePolicy
            );

            // 7) Herder & Connect runtime
            Herder herder = new StandaloneHerder(worker, kafkaClusterId, connectorClientConfigOverridePolicy);
            final Connect connect = new Connect(herder, rest);

            logger.info("Kafka Connect standalone worker has been initialized at {}", advertisedUrl);

            try {
                connect.start();

                // 8) Create our connector
                Map<String, String> connectorProps = connectorProps();
                FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>((error, info) -> {
                    if (error != null) {
                        logger.error("Failed to create connector", error);
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

                // 9) Block until stopped
                stopLatch.await();

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.warn("Connect thread interrupted, stopping.");
            } catch (Throwable t) {
                logger.error("Error while running Kafka Connect", t);
            } finally {
                try {
                    connect.stop();
                    connect.awaitStop();
                } catch (Exception e) {
                    logger.warn("Error stopping Connect", e);
                }
                try {
                    offsetStore.stop();
                } catch (Exception e) {
                    logger.warn("Error stopping FileOffsetBackingStore", e);
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
                logger.warn("Interrupted while waiting for Connect thread to stop");
            }
        }
    }

    public void deleteOffsetsFile() {
        // Using the field here eliminates any “write-only” warning for offsetsFilename
        if (!new File(offsetsFilename).delete()) {
            logger.debug("Offsets file {} did not exist or could not be deleted", offsetsFilename);
        }
    }

    private Map<String, String> workerProps() {
        Map<String, String> p = new HashMap<>();
        p.put("bootstrap.servers", bootstrapServer);

        // Converters
        p.put("key.converter", JsonConverter.class.getName());
        p.put("value.converter", JsonConverter.class.getName());
        p.put("internal.key.converter", JsonConverter.class.getName());
        p.put("internal.value.converter", JsonConverter.class.getName());
        p.put("key.converter.schemas.enable", "true");
        p.put("value.converter.schemas.enable", "true");
        p.put("internal.key.converter.schemas.enable", "true");
        p.put("internal.value.converter.schemas.enable", "true");

        // REST (you can override via props if needed)
        p.put("rest.port", "8086");
        p.put("rest.host.name", "127.0.0.1");

        // Offsets file
        p.put("offset.storage.file.filename", offsetsFilename);
        p.put("offset.flush.interval.ms", "10000");
        return p;
    }

    private Map<String, String> connectorProps() {
        Map<String, String> p = new HashMap<>();
        p.put("name", "RssSourceConnectorDemo");
        p.put("tasks.max", Integer.toString(maxTasks));
        p.put("sleep.seconds", "2");
        p.put("connector.class", "org.kaliy.kafka.connect.rss.RssSourceConnector");
        p.put("rss.urls", urls);
        p.put("topic", "test_topic");
        return p;
    }
}
