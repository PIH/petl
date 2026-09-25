package org.pih.petl;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class PhaseTimerTest {

    @Test
    public void shouldAccumulateTimeForEachPhaseInOrder() throws Exception {
        PhaseTimer timer = new PhaseTimer();
        timer.start("setup");
        Thread.sleep(20);
        timer.start("copy");
        Assert.assertEquals("copy", timer.getCurrentPhase());
        Thread.sleep(20);
        timer.start("setup");
        Thread.sleep(20);
        timer.stop();
        Assert.assertNull(timer.getCurrentPhase());
        Assert.assertEquals(Arrays.asList("setup", "copy"), new ArrayList<>(timer.getPhaseMillis().keySet()));
        Assert.assertTrue(timer.getPhaseMillis().get("setup") >= 40);
        Assert.assertTrue(timer.getPhaseMillis().get("copy") >= 20);
        Assert.assertTrue(timer.toString().startsWith("setup: 0."));
        Assert.assertTrue(timer.toString().contains(", copy: 0."));
    }
}
