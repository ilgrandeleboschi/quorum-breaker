package io.grove.quorumbreaker.redis.spring;

import io.grove.quorumbreaker.cluster.ClusterCoordinator;
import io.grove.quorumbreaker.redis.RedisClusterCoordinator;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class QuorumBreakerRedisAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(QuorumBreakerRedisAutoConfiguration.class))
            .withPropertyValues("spring.application.name=test-app");

    @Configuration(proxyBeanMethods = false)
    static class RedisClientConfig {
        static final RedisClient CLIENT = mock(RedisClient.class);

        static {
            RedisCommands mockCommands = mock(RedisCommands.class);
            RedisPubSubCommands mockRedisPubSubCommands = mock(RedisPubSubCommands.class);
            StatefulRedisConnection<String, String> connection = mock(StatefulRedisConnection.class);
            when(connection.sync()).thenReturn(mockCommands);
            StatefulRedisPubSubConnection<String, String> pubSubConnection = mock(StatefulRedisPubSubConnection.class);
            when(pubSubConnection.sync()).thenReturn(mockRedisPubSubCommands);

            when(CLIENT.connect()).thenReturn(connection);
            when(CLIENT.connectPubSub()).thenReturn(pubSubConnection);
        }

        @Bean
        RedisClient redisClient() {
            return CLIENT;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class RedisUriConfig {
        @Bean
        RedisURI redisUri() {
            return RedisURI.create("redis://localhost:6379");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomClusterCoordinatorConfig {
        static final ClusterCoordinator CUSTOM = mock(ClusterCoordinator.class);

        @Bean
        ClusterCoordinator customClusterCoordinator() {
            return CUSTOM;
        }
    }

    @Test
    void noBeansPresent_registersNoClusterCoordinator_butWarnsAboutIt() {
        contextRunner.run(context -> {
            assertEquals(0, context.getBeansOfType(ClusterCoordinator.class).size());
            assertEquals(1, context.getBeansOfType(InitializingBean.class).size(),
                    "should still warn that quorum-breaker is on the classpath but inactive");
        });
    }

    @Test
    void redisClientBean_present_wrapsIt() {
        contextRunner.withUserConfiguration(RedisClientConfig.class).run(context -> {
            assertEquals(1, context.getBeansOfType(ClusterCoordinator.class).size());
            assertInstanceOf(RedisClusterCoordinator.class, context.getBean(ClusterCoordinator.class));
            assertEquals(0, context.getBeansOfType(InitializingBean.class).size());
        });
    }

    @Test
    void bothBeansPresent_clientTakesPriorityOverUri() {
        contextRunner.withUserConfiguration(RedisClientConfig.class, RedisUriConfig.class).run(context -> {
            assertEquals(1, context.getBeansOfType(ClusterCoordinator.class).size(),
                    "must not create two coordinators when both a client and a URI are available");
            verify(RedisClientConfig.CLIENT, atLeastOnce()).connect();
        });
    }

    @Test
    void redisClientBean_present_butNoApplicationName_failsFast() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(QuorumBreakerRedisAutoConfiguration.class))
                .withUserConfiguration(RedisClientConfig.class)
                .run(context -> assertNotNull(context.getStartupFailure(),
                        "must fail context startup rather than silently default the namespace"));
    }

    @Test
    void userSuppliedClusterCoordinator_shortCircuitsEverythingElse() {
        contextRunner.withUserConfiguration(CustomClusterCoordinatorConfig.class,
                RedisClientConfig.class, RedisUriConfig.class).run(context -> {
            assertEquals(1, context.getBeansOfType(ClusterCoordinator.class).size());
            assertSame(CustomClusterCoordinatorConfig.CUSTOM, context.getBean(ClusterCoordinator.class));
            assertEquals(0, context.getBeansOfType(InitializingBean.class).size());
        });
    }

}
