package io.grove.quorumbreaker.spring;

import io.grove.quorumbreaker.breaker.QuorumBreakerRegistryBinder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties(prefix = "quorum-breaker")
public class QuorumBreakerProperties {

    private Duration windowTtlPadding = QuorumBreakerRegistryBinder.DEFAULT_WINDOW_TTL_PADDING;

}
