package io.grove.quorumbreaker.hazelcast.spring;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import io.grove.quorumbreaker.cluster.ClusterCoordinator;
import io.grove.quorumbreaker.hazelcast.HazelcastClusterCoordinator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration(proxyBeanMethods = false)
@AutoConfigureAfter(name = {
        "org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration",
        "org.springframework.boot.hazelcast.autoconfigure.HazelcastAutoConfiguration"
})
public class QuorumBreakerHazelcastAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(ClusterCoordinator.class)
    @ConditionalOnBean(HazelcastInstance.class)
    public ClusterCoordinator quorumBreakerClusterCoordinatorFromInstance(
            HazelcastInstance instance, @Value("${spring.application.name:}") String applicationName) {
        String namespace = requireNamespace(applicationName);
        log.info("QuorumBreaker: attaching to existing HazelcastInstance '{}' under namespace '{}'",
                instance.getName(), namespace);
        return HazelcastClusterCoordinator.wrap(instance, namespace);
    }

    @Bean
    @ConditionalOnMissingBean({ClusterCoordinator.class, HazelcastInstance.class})
    @ConditionalOnBean(Config.class)
    public ClusterCoordinator quorumBreakerClusterCoordinatorFromConfig(
            Config config, @Value("${spring.application.name:}") String applicationName) {
        String namespace = requireNamespace(applicationName);
        log.info("QuorumBreaker: starting embedded Hazelcast node with user-provided Config under namespace '{}'",
                namespace);
        return HazelcastClusterCoordinator.embedded(config, namespace);
    }

    private static String requireNamespace(String applicationName) {
        if (applicationName == null || applicationName.isBlank()) {
            throw new IllegalStateException("quorum-breaker-hazelcast-spring requires 'spring.application.name' " +
                    "to be set - it's used as the namespace that keeps this application's shared Hazelcast " +
                    "state from colliding with any other application on the same cluster");
        }
        return applicationName;
    }

    @Bean
    @ConditionalOnMissingBean({ClusterCoordinator.class, HazelcastInstance.class, Config.class})
    public InitializingBean quorumBreakerHazelcastMisconfigurationWarning() {
        return () -> log.warn("QuorumBreaker: quorum-breaker-hazelcast is on the classpath but no " +
                "HazelcastInstance or Config bean was found - staying inactive (plain local " +
                "Resilience4j behaviour). Define a HazelcastInstance or a 'com.hazelcast.config.Config' " +
                "bean to enable quorum-breaker");
    }

}
