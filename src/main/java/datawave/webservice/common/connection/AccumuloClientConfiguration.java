package datawave.webservice.common.connection;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.log4j.Logger;

public class AccumuloClientConfiguration {
    private Logger log = Logger.getLogger(AccumuloClientConfiguration.class);
    private Map<String,Map<String,String>> hintsByTable = new HashMap<>();
    private Map<String,ScannerBase.ConsistencyLevel> consistencyByTable = new HashMap<>();
    
    public AccumuloClientConfiguration() {}
    
    public AccumuloClientConfiguration(Map<String,Map<String,String>> hints) {
        this(hints, Collections.emptyMap());
    }
    
    public AccumuloClientConfiguration(Map<String,Map<String,String>> hints, Map<String,ScannerBase.ConsistencyLevel> levels) {
        for (String table : hints.keySet()) {
            putHints(table, hints.get(table));
        }
        for (String table : levels.keySet()) {
            setConsistency(table, levels.get(table));
        }
    }
    
    public void apply(ScannerBase scanner, String tableName) {
        if (hintsByTable.containsKey(tableName)) {
            try {
                scanner.setExecutionHints(hintsByTable.get(tableName));
            } catch (Exception e) {
                log.warn("Failed to set execution hints for " + tableName, e);
            }
        }
        if (consistencyByTable.containsKey(tableName)) {
            scanner.setConsistencyLevel(consistencyByTable.get(tableName));
        }
    }
    
    public void setConsistency(String table, ScannerBase.ConsistencyLevel level) {
        consistencyByTable.put(table, level);
    }
    
    public void addHint(String table, String prop, String value) {
        addHints(table, Collections.singletonMap(prop, value));
    }
    
    public void addHints(String table, Map<String,String> hints) {
        if (hints != null && !hints.isEmpty()) {
            if (hintsByTable.containsKey(table)) {
                hintsByTable.get(table).putAll(hints);
            } else {
                hintsByTable.put(table, new HashMap<>(hints));
            }
        }
    }
    
    public void putHints(String table, Map<String,String> hints) {
        if (hints == null || hints.isEmpty()) {
            hintsByTable.remove(table);
        } else {
            hintsByTable.put(table, new HashMap<>(hints));
        }
    }
    
}
