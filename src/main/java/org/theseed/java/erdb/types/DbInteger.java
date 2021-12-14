/**
 *
 */
package org.theseed.java.erdb.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.theseed.java.erdb.DbValue;

/**
 * This object holds a basic integer type.  The default value is 0.
 *
 * @author Bruce Parrello
 *
 */
public class DbInteger extends DbValue {

    // FIELDS
    private int value;

    /**
     * Construct an integer with the default value.
     */
    public DbInteger() {
        this.value = 0;
    }

    /**
     * Construct an integer with the specified value.
     *
     * @param val		initial value
     */
    public DbInteger(int val) {
        this.value = val;
    }

    /**
     * @return the value of this object
     */
    public int get() {
        return this.value;
    }

    @Override
    protected void storeValue(PreparedStatement stmt, int idx) throws SQLException {
        stmt.setInt(idx, this.value);
    }

    @Override
    protected void fetchValue(ResultSet results, int idx) throws SQLException {
        this.value = results.getInt(idx);
    }

    @Override
    public int getInt() {
        return this.value;
    }

    @Override
    public double getDouble() {
        return (double) this.value;
    }

    @Override
    public String getString() {
        return Integer.toString(this.value);
    }

    /**
     * Store an integer value in this value holder.
     *
     * @param val	value to store
     */
    public void set(int val) {
        this.value = val;
        this.setNotNull();
    }

    @Override
    protected int getSqlType() {
        return Types.INTEGER;
    }

}
