package org.itfactory.kettle.steps.bigquerystream;

import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Batch container for the PDI BigQuery stream step
 *
 * @author deneraraujo
 * @since 08-07-2024
 */
public class BigQueryStreamBatch {
    private Multimap<Action, Map<String, Object>> batch;
    int counter;

    /**
     * Initialise batch
     */
    public BigQueryStreamBatch() {
        batch = HashMultimap.create();
        counter = 0;
    }

    /**
     * Add row to a batch
     */
    public void addRow(Map<String, Object> rowContent, Action action) {
        batch.put(action, rowContent);
        counter++;
    }

    /**
     * Get batch total size
     */
    public int size(){
        return counter;
    }

    /**
     * Get rows from batch, filtered by action
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object>[] getRows(Action action) {
        if (batch.size() > 0) {
            return batch.get(action).toArray(new Map[0]);
        }
        return (Map<String, Object>[]) new Map[0];
    }

    /**
     * Action enum
     */
    public enum Action {
        Add, Update, None, Temp
    }
}