package datawave.util.time;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

/**
 * Utility for measuring the time taken to perform some operation.
 */
public class TraceStopwatch {
    
    protected final String description;
    protected final Stopwatch sw;
    protected TraceScope span;
    
    public TraceStopwatch(String description) {
        Preconditions.checkNotNull(description);
        
        this.description = description;
        this.sw = Stopwatch.createUnstarted();
    }
    
    public String description() {
        return this.description;
    }
    
    public boolean isRunning() {
        // Wild on you
        return this.sw.isRunning();
    }
    
    public void start() {
        span = Trace.startSpan(description);
        this.sw.start();
    }
    
    public void data(String name, String value) {
        span.getSpan().addKVAnnotation(name, value);
    }
    
    public void stop() {
        this.sw.stop();
        
        if (null != span) {
            span.getSpan().stop();
        }
    }
    
    public long elapsed(TimeUnit desiredUnit) {
        return sw.elapsed(desiredUnit);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(span.hashCode()).append(description).append(sw.hashCode()).toHashCode();
    }
    
    @Override
    public String toString() {
        return sw.toString();
    }
}
