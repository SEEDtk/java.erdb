/**
 *
 */
package org.theseed.java.erdb.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
    protected void store(PreparedStatement stmt, int idx) throws SQLException {
        stmt.setInt(idx, this.value);
    }

    @Override
    protected void fetch(ResultSet results, int idx) throws SQLException {
        this.value = results.getInt(idx);
    }

}
