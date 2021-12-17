/**
 *
 */
package org.theseed.erdb.utils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.theseed.java.erdb.DbRecord;

/**
 * This is a stream collector that converts a stream of database records to a map from
 * a specified key field to the records themselves.
 *
 * @author Bruce Parrello
 *
 */
public class MapCollector implements Collector<DbRecord, Map<String, DbRecord>, Map<String, DbRecord>> {

    // FIELDS
    /** specification for key field */
    private String keySpec;

    @Override
    public Supplier<Map<String, DbRecord>> supplier() {
        return HashMap::new;
    }

    @Override
    public BiConsumer<Map<String, DbRecord>, DbRecord> accumulator() {
        // Note we cannot throw a checked exception, so we make any SQLExceptions unchecked
        return (map, record) -> {
            try {
                String key = record.getString(this.keySpec);
                map.put(key, record);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public BinaryOperator<Map<String, DbRecord>> combiner() {
        return (map1, map2) -> {
            map1.putAll(map2);
            return map1;
        };
    }

    @Override
    public Function<Map<String, DbRecord>, Map<String, DbRecord>> finisher() {
        return x -> x;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.IDENTITY_FINISH, Characteristics.UNORDERED);
    }

    /**
     * Specify the key for the map.
     *
     * @param value		name of the key field (table.name)
     */
    protected void setKeySpec(String value) {
        this.keySpec = value;
    }

}
