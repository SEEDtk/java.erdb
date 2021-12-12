/**
 *
 */
package org.theseed.java.erdb.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
    protected void store(PreparedStatement stmt, int idx) throws SQLException {
        stmt.setString(idx, this.value);
    }

    @Override
    protected void fetch(ResultSet results, int idx) throws SQLException {
        this.value = results.getString(idx);
    }

}
