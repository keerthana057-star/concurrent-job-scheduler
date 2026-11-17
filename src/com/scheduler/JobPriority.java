package com.scheduler;

/**
 * Enum representing job priority levels.
 * Higher enum values have higher priority.
 */
public enum JobPriority {
    LOW(1),
    MEDIUM(2), 
    HIGH(3);
    
    private final int level;
    
    JobPriority(int level) {
        this.level = level;
    }
    
    public int getLevel() {
        return level;
    }
    
    /**
     * Returns whether this priority is higher than another.
     */
    public boolean isHigherThan(JobPriority other) {
        return this.level > other.level;
    }
}
