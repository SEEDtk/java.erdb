/**
 *
 */
package org.theseed.erdb.utils;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import org.theseed.java.erdb.DbRecord;

/**
 * Create a set out of a database record stream, based on a specified key field.
 *
 * @author Bruce Parrello
 *
 */
public class SetCollector implements Collector<DbRecord, Set<String>, Set<String>> {

    // FIELDS
    /** specification for key field */
    private String keySpec;

    @Override
    public Supplier<Set<String>> supplier() {
        return HashSet::new;
    }

    @Override
    public BiConsumer<Set<String>, DbRecord> accumulator() {
        return (set, record) -> {
            try {
                String key = record.getString(this.keySpec);
                set.add(key);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public BinaryOperator<Set<String>> combiner() {
        return (set1, set2) -> {
            set1.addAll(set2);
            return set1;
        };
    }

    @Override
    public Function<Set<String>, Set<String>> finisher() {
        return x -> x;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.IDENTITY_FINISH, Characteristics.UNORDERED);
    }

    /**
     * Specify the key field used to form the set.
     *
     * @param key	specifier (table.field) for the desired key
     */
    public void setKeySpec(String key) {
        this.keySpec = key;
    }

}
