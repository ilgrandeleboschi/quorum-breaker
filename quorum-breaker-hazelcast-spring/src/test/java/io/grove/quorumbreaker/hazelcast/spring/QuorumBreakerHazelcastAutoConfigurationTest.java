package io.grove.quorumbreaker.hazelcast.spring;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.grove.quorumbreaker.cluster.ClusterCoordinator;
import io.grove.quorumbreaker.hazelcast.HazelcastClusterCoordinator;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class QuorumBreakerHazelcastAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(QuorumBreakerHazelcastAutoConfiguration.class))
            .withPropertyValues("spring.application.name=test-app");

    @Configuration(proxyBeanMethods = false)
    static class HazelcastInstanceConfig {
        static final HazelcastInstance INSTANCE = mock(HazelcastInstance.class);

        static {
            IMap mock = mock(IMap.class);
            when(INSTANCE.getMap(anyString())).thenReturn(mock);
        }

        @Bean
        HazelcastInstance hazelcastInstance() {
            return INSTANCE;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class HazelcastConfigConfig {
        @Bean
        Config hazelcastConfig() {
            Config config = new Config();
            config.setProperty("hazelcast.logging.type", "none");
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            return config;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomClusterCoordinatorConfig {
        @SuppressWarnings("unchecked")
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
    void hazelcastInstanceBean_present_wrapsIt() {
        contextRunner.withUserConfiguration(HazelcastInstanceConfig.class).run(context -> {
            assertEquals(1, context.getBeansOfType(ClusterCoordinator.class).size());
            assertInstanceOf(HazelcastClusterCoordinator.class, context.getBean(ClusterCoordinator.class));
            assertEquals(0, context.getBeansOfType(InitializingBean.class).size());
        });
    }

    @Test
    void bothBeansPresent_instanceTakesPriorityOverConfig() {
        contextRunner.withUserConfiguration(HazelcastInstanceConfig.class, HazelcastConfigConfig.class).run(context -> {
            assertEquals(1, context.getBeansOfType(ClusterCoordinator.class).size(),
                    "must not create two coordinators when both an instance and a config are available");
            // wrap() reads the shared IMap straight off the supplied instance; embedded() never
            // touches it, so this proves the instance path won, not the config path.
            verify(HazelcastInstanceConfig.INSTANCE, atLeastOnce()).getMap(anyString());
        });
    }

    @Test
    void hazelcastInstanceBean_present_butNoApplicationName_failsFast() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(QuorumBreakerHazelcastAutoConfiguration.class))
                .withUserConfiguration(HazelcastInstanceConfig.class)
                .run(context -> assertNotNull(context.getStartupFailure(),
                        "must fail context startup rather than silently default the namespace"));
    }

    @Test
    void userSuppliedClusterCoordinator_shortCircuitsEverythingElse() {
        contextRunner.withUserConfiguration(CustomClusterCoordinatorConfig.class,
                HazelcastInstanceConfig.class, HazelcastConfigConfig.class).run(context -> {
            assertEquals(1, context.getBeansOfType(ClusterCoordinator.class).size());
            assertSame(CustomClusterCoordinatorConfig.CUSTOM, context.getBean(ClusterCoordinator.class));
            assertEquals(0, context.getBeansOfType(InitializingBean.class).size());
        });
    }

}
