/**
 *
 */
package org.theseed.java.erdb.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;

import org.theseed.java.erdb.DbValue;

/**
 * This object holds a basic floating-point value.  The default is the null date.
 *
 * @author Bruce Parrello
 *
 */
public class DbDate extends DbValue {

    // FIELDS
    /** value of this object */
    private Instant value;
    /** default date */
    private static final Instant DEFAULT = Instant.EPOCH;
    /** conversion factor */
    private static final int SECONDS_PER_DAY = 24 * 60 * 60;

    /**
     * Create a date with the default value.
     */
    public DbDate() {
        this.value = DEFAULT;
    }

    /**
     * Create a date with the specified value.
     */
    public DbDate(Instant val) {
        this.value = val;
    }

    /**
     * @return the value of this object.
     */
    public Instant get() {
        return this.value;
    }

    @Override
    protected void storeValue(PreparedStatement stmt, int idx) throws SQLException {
        // Convert the date to a julian fraction.
        double julian = this.value.getEpochSecond() / (double) SECONDS_PER_DAY;
        stmt.setDouble(idx, julian);
    }

    @Override
    protected void fetchValue(ResultSet results, int idx) throws SQLException {
        // Convert the julian fraction to a date.
        double julian = results.getDouble(idx);
        long seconds = (long) (julian * SECONDS_PER_DAY);
        this.value = Instant.ofEpochSecond(seconds);
    }

    @Override
    public int getInt() {
        return (int) (this.value.getEpochSecond() / SECONDS_PER_DAY);
    }

    @Override
    public double getDouble() {
        return this.value.getEpochSecond() / (double) SECONDS_PER_DAY;
    }

    @Override
    public String getString() {
        return this.value.toString();
    }

    @Override
    protected int getSqlType() {
        return Types.DOUBLE;
    }

    /**
     * Store a date in this value holder.
     *
     * @param date		date to store
     */
    public void set(Instant date) {
        this.value = date;
        this.setNotNull();
    }

}
