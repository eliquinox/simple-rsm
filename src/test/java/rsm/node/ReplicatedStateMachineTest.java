package rsm.node;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReplicatedStateMachineTest {

    @Test
    void shouldSetAndGetValue() {
        final ReplicatedStateMachine replicatedStateMachine = new ReplicatedStateMachine();
        replicatedStateMachine.setValue(10L);
        final long actualValue = replicatedStateMachine.getValue();
        assertEquals(10L, actualValue);
    }
}