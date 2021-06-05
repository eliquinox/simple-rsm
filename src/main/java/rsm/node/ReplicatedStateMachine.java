package rsm.node;

import org.agrona.collections.MutableLong;

public class ReplicatedStateMachine {

    private final MutableLong value = new MutableLong();

    public void setValue(final long value) {
        this.value.set(value);
    }

    public long getValue() {
        return value.get();
    }
}
