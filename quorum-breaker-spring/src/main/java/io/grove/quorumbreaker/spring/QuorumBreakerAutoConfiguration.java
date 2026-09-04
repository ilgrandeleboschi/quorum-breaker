package io.grove.quorumbreaker.spring;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.grove.quorumbreaker.breaker.QuorumBreakerRegistryBinder;
import io.grove.quorumbreaker.cluster.ClusterCoordinator;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(QuorumBreakerProperties.class)
public class QuorumBreakerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(Clock.class)
    public Clock quorumBreakerClock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnBean(ClusterCoordinator.class)
    public QuorumBreakerRegistryBinder quorumBreakerRegistryBinder(
            CircuitBreakerRegistry registry, ClusterCoordinator cluster, Clock clock,
            QuorumBreakerProperties properties) {
        QuorumBreakerRegistryBinder binder = new QuorumBreakerRegistryBinder(
                registry, cluster, clock, properties.getWindowTtlPadding());
        binder.bindAll();
        return binder;
    }

    @Bean
    @ConditionalOnBean(QuorumBreakerRegistryBinder.class)
    public CircuitBreaker quorumBreakerClusterCoordinatorGuard(QuorumBreakerRegistryBinder binder) {
        return binder.clusterCoordinatorGuard();
    }

    @Bean
    @ConditionalOnClass(TaggedCircuitBreakerMetrics.class)
    @ConditionalOnBean({QuorumBreakerRegistryBinder.class, MeterRegistry.class})
    public InitializingBean quorumBreakerClusterCoordinatorGuardMetrics(
            QuorumBreakerRegistryBinder binder, MeterRegistry meterRegistry) {
        return () -> {
            CircuitBreaker guard = binder.clusterCoordinatorGuard();
            CircuitBreakerRegistry guardRegistry = CircuitBreakerRegistry.ofDefaults();
            guardRegistry.circuitBreaker(guard.getName());
            guardRegistry.replace(guard.getName(), guard);
            TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(guardRegistry).bindTo(meterRegistry);
        };
    }

}
