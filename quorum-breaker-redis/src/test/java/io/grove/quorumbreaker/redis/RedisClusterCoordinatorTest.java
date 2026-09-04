package io.grove.quorumbreaker.redis;

import io.grove.quorumbreaker.gate.ClaimParams;
import io.grove.quorumbreaker.gate.WindowState;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class RedisClusterCoordinatorTest {

    private record Rig(RedisClusterCoordinator coordinator, RedisCommands<String, String> sync,
                        RedisPubSubCommands<String, String> pubSubSync,
                        StatefulRedisConnection<String, String> connection,
                        StatefulRedisPubSubConnection<String, String> pubSubConnection) {
    }

    private static Rig newRig(String namespace) {
        RedisCommands<String, String> sync = mock(RedisCommands.class);
        when(sync.scriptLoad(anyString())).thenReturn("initial-sha");
        StatefulRedisConnection<String, String> connection = mock(StatefulRedisConnection.class);
        when(connection.sync()).thenReturn(sync);

        RedisPubSubCommands<String, String> pubSubSync = mock(RedisPubSubCommands.class);
        StatefulRedisPubSubConnection<String, String> pubSubConnection = mock(StatefulRedisPubSubConnection.class);
        when(pubSubConnection.sync()).thenReturn(pubSubSync);

        RedisClusterCoordinator coordinator = RedisClusterCoordinator.wrap(connection, pubSubConnection, namespace);
        return new Rig(coordinator, sync, pubSubSync, connection, pubSubConnection);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    void blankNamespace_isRejected(String namespace) {
        StatefulRedisConnection<String, String> connection = mock(StatefulRedisConnection.class);
        StatefulRedisPubSubConnection<String, String> pubSubConnection = mock(StatefulRedisPubSubConnection.class);

        assertThrows(IllegalArgumentException.class,
                () -> RedisClusterCoordinator.wrap(connection, pubSubConnection, namespace));
    }

    @Test
    void windowKeyPrefix_andTopicPrefix_areNamespacedAndDisjoint() {
        assertEquals("quorumbreaker:orders-service:windows:", RedisClusterCoordinator.windowKeyPrefix("orders-service"));
        assertEquals("quorumbreaker:orders-service:events:", RedisClusterCoordinator.topicPrefix("orders-service"));
    }

    @Test
    void casSet_reloadsScriptAndRetriesViaEval_onNoScript() {
        Rig rig = newRig("ns");
        ClaimParams params = new ClaimParams(2, 50f, 100f);
        WindowState existing = WindowState.fresh("gen-1").claim();

        when(rig.sync().get("quorumbreaker:ns:windows:key-1")).thenReturn(existing.toString());
        when(rig.sync().evalsha(anyString(), any(ScriptOutputType.class), any(String[].class), anyString(), anyString(), anyString()))
                .thenThrow(new RedisNoScriptException("NOSCRIPT No matching script. Please use EVAL."));
        when(rig.sync().eval(anyString(), any(ScriptOutputType.class), any(String[].class), anyString(), anyString(), anyString()))
                .thenReturn(1L);

        RedisClusterCoordinator.ClaimResult result = rig.coordinator().claimOnce("key-1", params, 30_000);

        assertTrue(result.granted(), "the fallback eval() should still grant the claim after a NOSCRIPT miss");
        verify(rig.sync(), times(2)).scriptLoad(anyString());
        verify(rig.sync()).eval(anyString(), any(ScriptOutputType.class), any(String[].class), anyString(), anyString(), anyString());
    }

    @Test
    void subscribe_secondListenerOnSameChannel_doesNotResubscribe_andUnsubscribeOnlyOnLastListener() throws Exception {
        Rig rig = newRig("ns");

        AutoCloseable subscriptionOne = rig.coordinator().subscribe("chan", msg -> {
        }).toCompletableFuture().join();
        verify(rig.pubSubSync(), times(1)).subscribe("quorumbreaker:ns:events:chan");

        AutoCloseable subscriptionTwo = rig.coordinator().subscribe("chan", msg -> {
        }).toCompletableFuture().join();
        verify(rig.pubSubSync(), times(1)).subscribe(anyString());

        subscriptionOne.close();
        verify(rig.pubSubSync(), never()).unsubscribe(anyString());

        subscriptionTwo.close();
        verify(rig.pubSubSync(), times(1)).unsubscribe("quorumbreaker:ns:events:chan");
    }

    @Test
    void close_onWrappedConnections_doesNotCloseThem() {
        Rig rig = newRig("ns");

        rig.coordinator().close();

        verify(rig.connection(), never()).close();
        verify(rig.pubSubConnection(), never()).close();
    }

    @Test
    void wrap_client_connectPubSubFails_closesAlreadyOpenedConnection() {
        RedisClient client = mock(RedisClient.class);
        StatefulRedisConnection<String, String> connection = mock(StatefulRedisConnection.class);
        when(client.connect()).thenReturn(connection);
        when(client.connectPubSub()).thenThrow(new RuntimeException("boom"));

        assertThrows(RuntimeException.class, () -> RedisClusterCoordinator.wrap(client, "ns"));

        verify(connection).close();
    }

    @Test
    void wrap_client_constructorFails_closesBothConnections() {
        RedisCommands<String, String> sync = mock(RedisCommands.class);
        when(sync.scriptLoad(anyString())).thenThrow(new RuntimeException("redis unreachable"));
        StatefulRedisConnection<String, String> connection = mock(StatefulRedisConnection.class);
        when(connection.sync()).thenReturn(sync);
        StatefulRedisPubSubConnection<String, String> pubSubConnection = mock(StatefulRedisPubSubConnection.class);
        RedisPubSubCommands mockCommand = mock(RedisPubSubCommands.class);
        when(pubSubConnection.sync()).thenReturn(mockCommand);

        RedisClient client = mock(RedisClient.class);
        when(client.connect()).thenReturn(connection);
        when(client.connectPubSub()).thenReturn(pubSubConnection);

        assertThrows(RuntimeException.class, () -> RedisClusterCoordinator.wrap(client, "ns"));

        verify(connection).close();
        verify(pubSubConnection).close();
    }

}
