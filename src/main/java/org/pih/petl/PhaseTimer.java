package org.pih.petl;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tracks the time spent in each named phase of a job.  Starting a phase ends the current phase.
 * Time is accumulated if a phase is started more than once.  Phases are started and stopped by the job's thread,
 * and the current phase may be read from other threads, eg. to log progress.
 */
public class PhaseTimer {

    private final long createdMillis = System.currentTimeMillis();
    private final Map<String, Long> phaseMillis = new LinkedHashMap<>();
    private volatile String currentPhase;
    private volatile long currentPhaseStart;

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

    /**
     * @return how long the current phase has been in progress, or 0 if none
     */
    public long getCurrentPhaseMillis() {
        return currentPhase == null ? 0 : System.currentTimeMillis() - currentPhaseStart;
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
