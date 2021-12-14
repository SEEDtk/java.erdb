/**
 *
 */
package org.theseed.java.erdb;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This is the base class for all values transmitted to and from the database.  A database query
 * returns a record consisting of a map from field names to DbValues, and complex parameters are stored
 * via DbValues.
 *
 * Because of the need to be compatible with SQLite, we require all values to be implemented as integer,
 * real, string, or blob.  This allows the value object to be database-independent and greatly simplifies
 * implementation; however, it means that native SQL processing is more difficult.
 *
 * @author Bruce Parrello
 *
 */
public abstract class DbValue {

    // FIELDS
    /** TRUE if this value is null */
    private boolean nullFlag;

    /**
     * Construct the database value.
     */
    public DbValue() {
        this.nullFlag = false;
    }

    /**
     * Store the value in a prepared statement as a parameter.
     *
     * @param stmt		prepared statement into which this value should be stored
     * @param idx		index of the parameter mark it is replacing
     *
     * @throws SQLException
     */
    protected final void store(PreparedStatement stmt, int idx) throws SQLException {
        if (this.isNull())
            stmt.setNull(idx, this.getSqlType());
        else
            this.storeValue(stmt, idx);
    }

    /**
     * Store the (non-null) value in a prepared statement as a parameter.
     *
     * @param stmt		prepared statement into which this value should be stored
     * @param idx		index of the parameter mark it is replacing
     *
     * @throws SQLException
     */
    protected abstract void storeValue(PreparedStatement stmt, int idx) throws SQLException;

    /**
     * @return the SQL type of this field.
     */
    protected abstract int getSqlType();

    /**
     * Retrieve the value from a query result.
     *
     * @param result	query result set
     * @param idx		index of the value in the result set
     *
     * @throws SQLException
     */
    protected final void fetch(ResultSet results, int idx) throws SQLException {
        this.fetchValue(results, idx);
        this.checkNull(results);
    }

    /**
     * Retrieve the value from a query result.  (Does not check for null.)
     *
     * @param result	query result set
     * @param idx		index of the value in the result set
     *
     * @throws SQLException
     */
    protected abstract void fetchValue(ResultSet results, int idx) throws SQLException;

    /**
     * @return this value as an integer
     *
     * @throws SQLException
     */
    public abstract int getInt() throws SQLException;

    /**
     * @return this value as an floating-point number
     *
     * @throws SQLException
     */
    public abstract double getDouble() throws SQLException;

    /**
     * @return this value as a string
     *
     * @throws SQLException
     */
    public abstract String getString() throws SQLException;

    /**
     * Store a null in this value holder.
     */
    public void setNull() {
        this.nullFlag = true;
    }

    /**
     * Denote this value is non-null.
     */
    protected void setNotNull() {
        this.nullFlag = false;
    }

    /**
     * @return TRUE if this value is null
     */
    public boolean isNull() {
        return this.nullFlag;
    }

    /**
     * This can be called immediately after a fetch to indicate whether the value fetched was null.
     *
     * @param results	current result set
     *
     * @throws SQLException
     */
    protected void checkNull(ResultSet results) throws SQLException {
        this.nullFlag = results.wasNull();
    }


}
