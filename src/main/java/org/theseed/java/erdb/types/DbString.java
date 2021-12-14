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
 * This object holds a basic string value.  The default is the empty string.
 *
 * @author Bruce Parrello
 *
 */
public class DbString extends DbValue {

    // FIELDS
    /** value of this object */
    private String value;

    /**
     * Create a string value with the default value.
     */
    public DbString() {
        this.value = "";
    }

    /**
     * Create a string value with the specified value.
     */
    public DbString(String val) {
        this.value = val;
    }

    /**
     * @return the value of this object.
     */
    public String get() {
        return this.value;
    }

    @Override
    protected void storeValue(PreparedStatement stmt, int idx) throws SQLException {
        stmt.setString(idx, this.value);
    }

    @Override
    protected void fetchValue(ResultSet results, int idx) throws SQLException {
        this.value = results.getString(idx);
    }

    @Override
    public int getInt() throws SQLException {
        throw new SQLException("Cannot represent a STRING value as an integer.");
    }

    @Override
    public double getDouble() throws SQLException {
        throw new SQLException("Cannot represent a STRING value as floating-point.");
    }

    @Override
    public String getString() {
        return this.value;
    }

    /**
     * Store the specified string in this value.
     *
     * @param val	string value to store
     */
    public void set(String val) {
        this.value = val;
        this.setNotNull();
    }

    @Override
    protected int getSqlType() {
        return Types.VARCHAR;
    }

}
