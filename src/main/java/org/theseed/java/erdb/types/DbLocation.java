/**
 *
 */
package org.theseed.java.erdb.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.theseed.java.erdb.DbValue;
import org.theseed.locations.Location;

/**
 * This object holds a location value.  The default is essentially a null location, but it is thoroughly
 * meaningless.  The location is stored as a string.  To enable comparisons, the string is a fixed length
 * of 62 characters:  31 for the contig ID, 10 for the left position (1-based), 1 for the strand, and 9
 * for the length.  The numbers are all zero-filled.
 *
 * @author Bruce Parrello
 *
 */
public class DbLocation extends DbValue {

    // FIELDS
    /** value of this object */
    private Location value;
    /** default location value */
    private static final Location DEFAULT = Location.create("", 0, 0);

    /**
     * Create a blank location value.
     */
    public DbLocation() {
        this.value = DEFAULT;
    }

    /**
     * Create a floating-point value with the specified value.
     *
     * @param val		location to use
     */
    public DbLocation(Location val) {
        this.value = val;
    }

    /**
     * @return the value of this object.
     */
    public Location get() {
        return this.value;
    }

    @Override
    protected void storeValue(PreparedStatement stmt, int idx) throws SQLException {
        // Format the location as a string.
        String locString = locToString(this.value);
        stmt.setString(idx, locString);
    }

    @Override
    protected void fetchValue(ResultSet results, int idx) throws SQLException {
        String locString = results.getString(idx);
        this.value = stringToLoc(locString);
    }

    /**
     * @return the comparable string representation of this location
     *
     * @param val	location to convert to a string
     */
    protected static String locToString(Location val) {
        String retVal = String.format("%-41s|%010d%c%09d", val.getContigId(),
                val.getLeft(), val.getDir(), val.getLength());
        return retVal;
    }

    /**
     * @return the location described by a comparable string representation
     *
     * @param val	string to convert to a location
     */
    protected static Location stringToLoc(String val) {
        String[] parts = StringUtils.split(val, '|');
        int left = Integer.valueOf(parts[1].substring(0, 10));
        String dir = parts[1].substring(10, 11);
        int length = Integer.valueOf(parts[1].substring(11));
        Location retVal = Location.create(StringUtils.trimToEmpty(parts[0]), dir, left, left + length - 1);
        return retVal;
    }

    @Override
    public int getInt() throws SQLException {
        throw new SQLException("Cannot represent a LOCATION value as an integer.");
    }

    @Override
    public double getDouble() throws SQLException {
        throw new SQLException("Cannot represent a LOCATION value as floating-point.");
    }

    @Override
    public String getString() throws SQLException {
        return this.value.toString();
    }

    /**
     * Store a location in this value holder.
     *
     * @param loc		location to store
     */
    public void set(Location loc) {
        this.value = loc;
        this.setNotNull();
    }

    @Override
    protected int getSqlType() {
        return Types.VARCHAR;
    }

}
