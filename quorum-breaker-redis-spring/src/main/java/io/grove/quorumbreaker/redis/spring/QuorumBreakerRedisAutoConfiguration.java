package io.grove.quorumbreaker.redis.spring;

import io.grove.quorumbreaker.cluster.ClusterCoordinator;
import io.grove.quorumbreaker.redis.RedisClusterCoordinator;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration(proxyBeanMethods = false)
public class QuorumBreakerRedisAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(ClusterCoordinator.class)
    @ConditionalOnBean(RedisClient.class)
    public ClusterCoordinator quorumBreakerClusterCoordinatorFromClient(
            RedisClient client, @Value("${spring.application.name:}") String applicationName) {
        String namespace = requireNamespace(applicationName);
        log.info("QuorumBreaker: attaching to existing RedisClient under namespace '{}'", namespace);
        return RedisClusterCoordinator.wrap(client, namespace);
    }

    @Bean
    @ConditionalOnMissingBean({ClusterCoordinator.class, RedisClient.class})
    @ConditionalOnBean(RedisURI.class)
    public ClusterCoordinator quorumBreakerClusterCoordinatorFromUri(
            RedisURI uri, @Value("${spring.application.name:}") String applicationName) {
        String namespace = requireNamespace(applicationName);
        log.info("QuorumBreaker: starting a dedicated Redis client for '{}' under namespace '{}'", uri, namespace);
        return RedisClusterCoordinator.client(uri, namespace);
    }

    private static String requireNamespace(String applicationName) {
        if (applicationName == null || applicationName.isBlank()) {
            throw new IllegalStateException("quorum-breaker-redis-spring requires 'spring.application.name' " +
                    "to be set - it's used as the namespace that keeps this application's shared Redis " +
                    "state from colliding with any other application on the same server/cluster");
        }
        return applicationName;
    }

    @Bean
    @ConditionalOnMissingBean({ClusterCoordinator.class, RedisClient.class, RedisURI.class})
    public InitializingBean quorumBreakerRedisMisconfigurationWarning() {
        return () -> log.warn("QuorumBreaker: quorum-breaker-redis is on the classpath but no " +
                "RedisClient or RedisURI bean was found - staying inactive (plain local " +
                "Resilience4j behaviour). Define a RedisClient or a 'io.lettuce.core.RedisURI' " +
                "bean to enable quorum-breaker");
    }

}
