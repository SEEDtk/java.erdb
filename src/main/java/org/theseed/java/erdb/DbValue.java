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

    /**
     * Store the value in a prepared statement as a parameter.
     *
     * @param stmt		prepared statement into which this value should be stored
     * @param idx		index of the parameter mark it is replacing
     *
     * @throws SQLException
     */
    protected abstract void store(PreparedStatement stmt, int idx) throws SQLException;

    /**
     * Retrieve the value from a query result.
     *
     * @param result	query result set
     * @param idx		index of the value in the result set
     *
     * @throws SQLException
     */
    protected abstract void fetch(ResultSet results, int idx) throws SQLException;

}
