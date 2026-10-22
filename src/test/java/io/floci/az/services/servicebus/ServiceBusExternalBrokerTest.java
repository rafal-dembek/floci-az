package io.floci.az.services.servicebus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.floci.az.config.EmulatorConfig;
import io.floci.az.core.docker.ContainerBuilder;
import io.floci.az.core.docker.ContainerLifecycleManager;
import io.floci.az.services.eventhub.ArtemisTlsGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ServiceBusExternalBrokerTest {

    private static final String HOST = "127.0.0.1";
    private static final String NAMESPACE = "sbemulatorns";
    private static final int JOLOKIA_PORT = 8161;

    private final EmulatorConfig config = mock(EmulatorConfig.class);
    private final EmulatorConfig.ServiceBusConfig serviceBus = mock(EmulatorConfig.ServiceBusConfig.class);
    private final EmulatorConfig.ServiceBusExternalBrokerConfig externalBroker =
            mock(EmulatorConfig.ServiceBusExternalBrokerConfig.class);
    private final ContainerBuilder containerBuilder = mock(ContainerBuilder.class);
    private final ContainerLifecycleManager lifecycleManager = mock(ContainerLifecycleManager.class);
    private final ArtemisTlsGenerator tlsGenerator = mock(ArtemisTlsGenerator.class);
    private final ServiceBusConfigGenerator configGenerator = new ServiceBusConfigGenerator(config);
    private ServerSocket amqp;
    private ServiceBusNamespaceManager manager;

    @BeforeEach
    void setUp() throws IOException {
        // Accepts the namespace's AMQP connections without answering: enough to show the port is open.
        amqp = new ServerSocket(0, 0, InetAddress.getLoopbackAddress());
        EmulatorConfig.ServicesConfig services = mock(EmulatorConfig.ServicesConfig.class);
        when(config.services()).thenReturn(services);
        when(services.serviceBus()).thenReturn(serviceBus);
        when(serviceBus.maxDeliveryCount()).thenReturn(10);
        when(serviceBus.externalBroker()).thenReturn(externalBroker);
        when(externalBroker.host()).thenReturn(Optional.of(HOST));
        when(externalBroker.amqpPort()).thenReturn(amqp.getLocalPort());
        when(externalBroker.jolokiaPort()).thenReturn(JOLOKIA_PORT);
        manager = new ServiceBusNamespaceManager(
                config, containerBuilder, lifecycleManager, configGenerator, tlsGenerator);
    }

    @AfterEach
    void tearDown() throws IOException {
        manager.shutdownAll();
        amqp.close();
    }

    @Test
    void startsANamespaceOnTheExternalBrokerWithoutAContainer() {
        ServiceBusNamespaceManager.NamespaceState state = manager.startNamespace(NAMESPACE, 5673, 5674);

        assertNull(state.containerId());
        assertEquals(amqp.getLocalPort(), state.amqpHostPort());
        assertEquals(0, state.amqpsHostPort());
        assertEquals(HOST, state.jolokiaHost());
        assertEquals(JOLOKIA_PORT, state.jolokiaPort());
        assertFalse(state.mocked());
        assertTrue(manager.stopNamespace(NAMESPACE));
        verifyNoInteractions(containerBuilder, lifecycleManager, tlsGenerator);
    }

    @Test
    void externalBrokerServesOneNamespace() {
        manager.startNamespace(NAMESPACE, 5673, 5674);

        ServiceBusNamespaceManager.NamespaceStartException error = assertThrows(
                ServiceBusNamespaceManager.NamespaceStartException.class,
                () -> manager.startNamespace("other", 0, 0));

        assertTrue(error.getCause().getMessage().contains("'" + NAMESPACE + "'"));
        assertEquals(Set.of(NAMESPACE), manager.listNamespaces().keySet());
    }

    @Test
    void readsMessageCountsFromTheExternalBrokerAsItsConsoleUser() throws Exception {
        AtomicReference<String> authorization = new AtomicReference<>();
        AtomicReference<String> request = new AtomicReference<>();
        HttpServer jolokia = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        jolokia.createContext("/console/jolokia", exchange -> {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            request.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(exchange, "[{\"status\":200,\"value\":3},{\"status\":200,\"value\":1}]");
        });
        jolokia.start();
        try {
            when(externalBroker.jolokiaPort()).thenReturn(jolokia.getAddress().getPort());
            when(externalBroker.user()).thenReturn("admin");
            when(externalBroker.password()).thenReturn("secret");
            manager.startNamespace(NAMESPACE, 5673, 5674);

            ServiceBusNamespaceManager.MessageCounts counts = manager.getMessageCounts(NAMESPACE, "orders");

            assertEquals(new ServiceBusNamespaceManager.MessageCounts(3, 1), counts);
            assertEquals("Basic " + Base64.getEncoder().encodeToString(
                    "admin:secret".getBytes(StandardCharsets.UTF_8)), authorization.get());
            JsonNode reads = new ObjectMapper().readTree(request.get());
            assertTrue(reads.get(0).path("mbean").asText().startsWith(
                    "org.apache.activemq.artemis:broker=\"floci-az-servicebus-external\","));
        } finally {
            jolokia.stop(0);
        }
    }

    @Test
    void exportsTheFilesTheExternalBrokerNeeds(@TempDir Path dir) throws IOException {
        manager.exportBrokerFiles(dir);
        // Exporting again into the same directory, as a restarted one-shot export does, replaces them.
        manager.exportBrokerFiles(dir);

        assertEquals(configGenerator.generateExternal(), Files.readString(dir.resolve("etc/broker.xml")));
        assertTrue(Files.readString(dir.resolve("etc/management.xml")).contains("io.floci.az.artemis"));
        assertJar(dir.resolve("lib/floci-az-servicebus-extension.jar"),
                ServiceBusNamespaceManager.ARTEMIS_EXTENSION_RESOURCE);
        assertJar(dir.resolve("lib/proton-j-" + ArtemisPatchVersions.PROTON_J_VERSION + ".jar"),
                ServiceBusNamespaceManager.PROTON_PATCH_RESOURCE);
        assertJar(dir.resolve("lib/artemis-amqp-protocol-" + ArtemisPatchVersions.ARTEMIS_VERSION + ".jar"),
                ServiceBusNamespaceManager.ARTEMIS_AMQP_PATCH_RESOURCE);
    }

    private static void assertJar(Path exported, String resource) throws IOException {
        try (InputStream embedded = ServiceBusExternalBrokerTest.class.getResourceAsStream(resource)) {
            assertArrayEquals(embedded.readAllBytes(), Files.readAllBytes(exported));
        }
    }

    private static void respond(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
