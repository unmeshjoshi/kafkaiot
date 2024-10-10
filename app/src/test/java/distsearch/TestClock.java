package distsearch;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class TestClock implements Clock {
    private Instant now;

    public TestClock(Instant initialTime) {
        this.now = initialTime;
    }

    @Override
    public Instant now() {
        return now;
    }

    public void advanceTime(long amount, ChronoUnit unit) {
        now = now.plus(amount, unit);
    }
}