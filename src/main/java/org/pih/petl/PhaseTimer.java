package org.pih.petl;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tracks the time spent in each named phase of a job.  Starting a phase ends the current phase.
 * Time is accumulated if a phase is started more than once.
 */
public class PhaseTimer {

    private final long createdMillis = System.currentTimeMillis();
    private final Map<String, Long> phaseMillis = new LinkedHashMap<>();
    private String currentPhase;
    private long currentPhaseStart;

    public void start(String phase) {
        stop();
        currentPhase = phase;
        currentPhaseStart = System.currentTimeMillis();
    }

    public void stop() {
        if (currentPhase != null) {
            long elapsed = System.currentTimeMillis() - currentPhaseStart;
            phaseMillis.merge(currentPhase, elapsed, Long::sum);
            currentPhase = null;
        }
    }

    /**
     * @return the phase currently in progress, or null if none
     */
    public String getCurrentPhase() {
        return currentPhase;
    }

    public long getTotalMillis() {
        return System.currentTimeMillis() - createdMillis;
    }

    public Map<String, Long> getPhaseMillis() {
        return phaseMillis;
    }

    /**
     * @return each phase and its duration, eg. "setup: 1.2s, extract query: 2m 10s, bulk copy: 11m 40s"
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> e : phaseMillis.entrySet()) {
            sb.append(sb.length() == 0 ? "" : ", ").append(e.getKey()).append(": ").append(LogUtils.formatDuration(e.getValue()));
        }
        return sb.toString();
    }
}
