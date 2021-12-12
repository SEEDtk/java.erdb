/**
 *
 */
package org.theseed.java.erdb.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.theseed.java.erdb.DbValue;

/**
 * This object holds a basic floating-point value.  The default is 0.0.
 *
 * @author Bruce Parrello
 *
 */
public class DbDouble extends DbValue {

    // FIELDS
    /** value of this object */
    private double value;

    /**
     * Create a floating-point value with the default value.
     */
    public DbDouble() {
        this.value = 0.0;
    }

    /**
     * Create a floating-point value with the specified value.
     */
    public DbDouble(double val) {
        this.value = val;
    }

    /**
     * @return the value of this object.
     */
    public double get() {
        return this.value;
    }

    @Override
    protected void store(PreparedStatement stmt, int idx) throws SQLException {
        stmt.setDouble(idx, this.value);
    }

    @Override
    protected void fetch(ResultSet results, int idx) throws SQLException {
        this.value = results.getDouble(idx);
    }

}
