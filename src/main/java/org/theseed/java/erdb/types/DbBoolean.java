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
 * This object holds a basic boolean type.  FALSE is 0 and TRUE is 1.  The default value is FALSE.
 *
 * @author Bruce Parrello
 *
 */
public class DbBoolean extends DbValue {

    // FIELDS
    private boolean value;

    /**
     * Construct a boolean with the default value.
     */
    public DbBoolean() {
        this.value = false;
    }

    /**
     * Construct a boolean with the specified value.
     *
     * @param val		initial value
     */
    public DbBoolean(boolean val) {
        this.value = val;
    }

    /**
     * @return the value of this object
     */
    public boolean get() {
        return this.value;
    }

    @Override
    protected void storeValue(PreparedStatement stmt, int idx) throws SQLException {
        int flag = (this.value ? 1 : 0);
        stmt.setInt(idx, flag);
    }

    @Override
    protected void fetchValue(ResultSet results, int idx) throws SQLException {
        int flag = results.getInt(idx);
        this.value = (flag != 0);
    }

    @Override
    public int getInt() {
        return (this.value ? 1 : 0);
    }

    @Override
    public double getDouble() {
        return (this.value ? 1.0 : 0.0);
    }

    @Override
    public String getString() {
        return (this.value ? "Y" : "");
    }

    @Override
    protected int getSqlType() {
        return Types.INTEGER;
    }

    /**
     * Store a boolean value in this holder.
     *
     * @param b		value to store
     */
    public void set(boolean b) {
        this.value = b;
    }

}
