package io.floci.az.services.servicebus;

import io.floci.az.config.EmulatorConfig;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.IntConsumer;

/**
 * Lifecycle manager for Service Bus sidecar containers.
 *
 * When {@code mocked = false}, a namespace starts on its first entity-management
 * call -- not on boot. Namespaces can also be started explicitly via
 * PUT /{account}-servicebus/namespaces/{ns}. All running namespaces are stopped on shutdown.
 * With {@code external-broker.host} set, the namespace runs on that broker and no container is
 * started or reaped.
 *
 * <p>When {@code mocked = true}, no container is started and the namespace is management-plane
 * state only: there is no AMQP data plane.
 *
 * <p>With {@code external-broker.export-dir} set, floci-az only writes the external broker's
 * files there and exits.
 */
@ApplicationScoped
public class ServiceBusContainerManager {

    private static final Logger LOG = Logger.getLogger(ServiceBusContainerManager.class);
    private static final int EXPORTED = 0;
    private static final int EXPORT_FAILED = 1;

    private final EmulatorConfig config;
    private final ServiceBusNamespaceManager namespaceManager;
    private final ServiceBusTopologyLoader topologyLoader;
    private final IntConsumer exit;

    @Inject
    public ServiceBusContainerManager(EmulatorConfig config,
                                       ServiceBusNamespaceManager namespaceManager,
                                       ServiceBusTopologyLoader topologyLoader) {
        this(config, namespaceManager, topologyLoader, Quarkus::asyncExit);
    }

    ServiceBusContainerManager(EmulatorConfig config,
                               ServiceBusNamespaceManager namespaceManager,
                               ServiceBusTopologyLoader topologyLoader,
                               IntConsumer exit) {
        this.config = config;
        this.namespaceManager = namespaceManager;
        this.topologyLoader = topologyLoader;
        this.exit = exit;
    }

    void onStart(@Observes StartupEvent ev) {
        EmulatorConfig.ServiceBusConfig sb = config.services().serviceBus();
        if (!sb.enabled()) {
            return;
        }
        Optional<String> exportDir = sb.externalBroker().exportDir();
        if (exportDir.isPresent()) {
            exportBrokerFiles(Path.of(exportDir.get()));
            return;
        }
        boolean mocked = sb.mocked();
        boolean externalBroker = sb.externalBroker().host().isPresent();
        if (mocked) {
            LOG.info("Service Bus service mocked — skipping container startup");
            if (externalBroker) {
                LOG.warn("Service Bus is mocked, so external-broker.host is ignored");
            }
        } else if (!externalBroker) {
            try {
                int reaped = namespaceManager.reapOrphanedContainers();
                if (reaped > 0) {
                    LOG.infov("Removed {0} orphaned Service Bus container(s)", reaped);
                }
            } catch (Exception e) {
                LOG.errorf(e, "Could not reap orphaned Service Bus containers");
            }
        }

        if (loadTopology()) {
            if (sb.startOnBoot()) {
                LOG.info("Service Bus topology file takes precedence over start-on-boot");
            }
            return;
        }

        if (sb.startOnBoot()) {
            if (mocked) {
                namespaceManager.startMockedNamespace(ServiceBusNamespaceManager.DEFAULT_NAMESPACE);
            } else {
                try {
                    namespaceManager.startNamespace(
                            ServiceBusNamespaceManager.DEFAULT_NAMESPACE,
                            sb.amqpPort(), sb.amqpTlsPort());
                } catch (Exception e) {
                    LOG.errorf(e, "Could not start the default Service Bus namespace on boot");
                    namespaceManager.recordStartFailure(ServiceBusNamespaceManager.DEFAULT_NAMESPACE, e);
                }
            }
        } else if (!mocked) {
            LOG.info("Service Bus service enabled — namespace starts on first entity management call");
        }
    }

    private void exportBrokerFiles(Path dir) {
        try {
            namespaceManager.exportBrokerFiles(dir);
        } catch (IOException | RuntimeException e) {
            LOG.errorf(e, "Could not write the external Service Bus broker's files to %s", dir);
            exit.accept(EXPORT_FAILED);
            return;
        }
        LOG.infov("Wrote the external Service Bus broker''s files to {0}; exiting", dir);
        exit.accept(EXPORTED);
    }

    private boolean loadTopology() {
        try {
            return topologyLoader.load();
        } catch (Exception e) {
            LOG.errorf(e, "Could not load the Service Bus topology file");
            return false;
        }
    }

    @PreDestroy
    void onStop() {
        if (!config.services().serviceBus().enabled()) {
            return;
        }
        try {
            namespaceManager.shutdownAll();
        } catch (Exception e) {
            LOG.warnf(e, "Error stopping Service Bus namespace containers");
        }
    }
}
