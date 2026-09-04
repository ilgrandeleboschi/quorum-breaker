package io.grove.quorumbreaker.spring;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.grove.quorumbreaker.breaker.QuorumBreakerRegistryBinder;
import io.grove.quorumbreaker.cluster.ClusterCoordinator;
import io.grove.quorumbreaker.gate.ClaimParams;
import io.grove.quorumbreaker.gate.WindowOutcome;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuorumBreakerAutoConfigurationTest {

    private static final CircuitBreakerConfig CB_CONFIG = CircuitBreakerConfig.custom()
            .slidingWindowSize(2)
            .minimumNumberOfCalls(2)
            .waitDurationInOpenState(Duration.ofSeconds(10))
            .build();

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(QuorumBreakerAutoConfiguration.class));

    @Configuration(proxyBeanMethods = false)
    static class RegistryConfig {
        @Bean
        CircuitBreakerRegistry circuitBreakerRegistry() {
            CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(CB_CONFIG);
            registry.circuitBreaker("wired-in-before-context-refresh");
            return registry;
        }
    }

    private static final class NoOpClusterCoordinator implements ClusterCoordinator {
        @Override
        public CompletionStage<Void> publish(String channel, String message) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<AutoCloseable> subscribe(String channel, Consumer<String> listener) {
            return CompletableFuture.completedFuture(() -> {
            });
        }

        @Override
        public CompletionStage<Boolean> claim(String key, ClaimParams params, Duration ttl) {
            return CompletableFuture.completedFuture(false);
        }

        @Override
        public CompletionStage<WindowOutcome> trace(String key, ClaimParams params, boolean success, boolean slow, Duration ttl) {
            return CompletableFuture.completedFuture(WindowOutcome.PENDING);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ClusterCoordinatorConfig {
        static final ClusterCoordinator COORDINATOR = new NoOpClusterCoordinator();

        @Bean
        ClusterCoordinator clusterCoordinator() {
            return COORDINATOR;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FixedClockConfig {
        @Bean
        Clock clock() {
            return Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneId.of("UTC"));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MeterRegistryConfig {
        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @Test
    void noClusterCoordinatorBean_bindingStaysInactive_breakersAreUntouched() {
        contextRunner.withUserConfiguration(RegistryConfig.class).run(context -> {
            assertEquals(0, context.getBeansOfType(QuorumBreakerRegistryBinder.class).size());

            CircuitBreakerRegistry registry = context.getBean(CircuitBreakerRegistry.class);
            CircuitBreaker cb = registry.circuitBreaker("wired-in-before-context-refresh");
            assertFalse(cb.getClass().getName().startsWith("io.grove.quorumbreaker.breaker.QuorumBreaker"),
                    "without a ClusterCoordinator bean, breakers must stay plain local Resilience4j instances");
        });
    }

    @Test
    void clusterCoordinatorBeanPresent_bindsAndReplacesExistingBreakers() {
        contextRunner.withUserConfiguration(RegistryConfig.class, ClusterCoordinatorConfig.class).run(context -> {
            assertEquals(1, context.getBeansOfType(QuorumBreakerRegistryBinder.class).size());

            CircuitBreakerRegistry registry = context.getBean(CircuitBreakerRegistry.class);
            CircuitBreaker cb = registry.circuitBreaker("wired-in-before-context-refresh");
            assertEquals("io.grove.quorumbreaker.breaker.QuorumBreaker", cb.getClass().getName());
        });
    }

    @Test
    void clusterCoordinatorBeanPresent_exposesTheGuardBreakerAsABean() {
        contextRunner.withUserConfiguration(RegistryConfig.class, ClusterCoordinatorConfig.class).run(context -> {
            QuorumBreakerRegistryBinder binder = context.getBean(QuorumBreakerRegistryBinder.class);
            assertSame(binder.clusterCoordinatorGuard(), context.getBean(CircuitBreaker.class));
        });
    }

    @Test
    void noClusterCoordinatorBean_noGuardBreakerBeanEither() {
        contextRunner.withUserConfiguration(RegistryConfig.class).run(context ->
                assertEquals(0, context.getBeansOfType(CircuitBreaker.class).size()));
    }

    @Test
    void clockBean_defaultsToSystemUtc_whenAbsent() {
        contextRunner.run(context -> assertEquals(1, context.getBeansOfType(Clock.class).size()));
    }

    @Test
    void clockBean_userProvided_isRespectedNotOverridden() {
        contextRunner.withUserConfiguration(FixedClockConfig.class).run(context -> {
            assertEquals(1, context.getBeansOfType(Clock.class).size());
            assertEquals(Instant.parse("2026-01-01T00:00:00Z"), context.getBean(Clock.class).instant());
        });
    }

    @Test
    void windowTtlPadding_defaultsWhenNotConfigured() {
        contextRunner.withUserConfiguration(RegistryConfig.class, ClusterCoordinatorConfig.class).run(context -> {
            QuorumBreakerRegistryBinder binder = context.getBean(QuorumBreakerRegistryBinder.class);
            assertEquals(QuorumBreakerRegistryBinder.DEFAULT_WINDOW_TTL_PADDING, binder.windowTtlPadding());
        });
    }

    @Test
    void windowTtlPadding_userProvidedViaProperty_isRespected() {
        contextRunner.withUserConfiguration(RegistryConfig.class, ClusterCoordinatorConfig.class)
                .withPropertyValues("quorum-breaker.window-ttl-padding=5s")
                .run(context -> {
                    QuorumBreakerRegistryBinder binder = context.getBean(QuorumBreakerRegistryBinder.class);
                    assertEquals(Duration.ofSeconds(5), binder.windowTtlPadding());
                });
    }

    @Test
    void guardMetrics_bound_whenMeterRegistryAndMicrometerBothPresent() {
        contextRunner.withUserConfiguration(RegistryConfig.class, ClusterCoordinatorConfig.class, MeterRegistryConfig.class)
                .run(context -> {
                    MeterRegistry meterRegistry = context.getBean(MeterRegistry.class);
                    assertTrue(meterRegistry.getMeters().stream()
                                    .anyMatch(meter -> meter.getId().getName().startsWith("resilience4j.circuitbreaker")),
                            "the cluster-coordinator guard breaker should be bound to the MeterRegistry");
                });
    }

    @Test
    void guardMetrics_notBound_whenNoMeterRegistryBean() {
        contextRunner.withUserConfiguration(RegistryConfig.class, ClusterCoordinatorConfig.class)
                .run(context -> assertEquals(0, context.getBeansOfType(MeterRegistry.class).size()));
    }

    @Test
    void guardMetrics_notBound_whenMicrometerIntegrationClassAbsent() {
        contextRunner.withUserConfiguration(RegistryConfig.class, ClusterCoordinatorConfig.class, MeterRegistryConfig.class)
                .withClassLoader(new FilteredClassLoader(TaggedCircuitBreakerMetrics.class))
                .run(context -> {
                    MeterRegistry meterRegistry = context.getBean(MeterRegistry.class);
                    assertTrue(meterRegistry.getMeters().isEmpty());
                });
    }

}
