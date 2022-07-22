/**
 *
 */
package org.theseed.java.erdb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.theseed.java.erdb.types.DbDate;
import org.theseed.java.erdb.types.DbDoubleArray;
import org.theseed.java.erdb.types.DbLocation;
import org.theseed.locations.Location;

/**
 * This object represents a single record.  It contains the value holders for all the fields in the
 * record, mapped by field spec (table.field).
 *
 * @author Bruce Parrello
 *
 */
public class DbRecord {

    // FIELDS
    /** map of field specs to value holders */
    private Map<String, DbValue> valueMap;
    /** list of value holders in order */
    private List<DbValue> valueList;

    /**
     * Create the record from the current result.
     *
     * @param results		result set positioned on the current record
     * @param fieldNames	list of field names in order
     * @param fieldTypes	list of field types in order
     *
     * @throws SQLException
     */
    protected DbRecord(ResultSet results, List<String> fieldNames, List<DbType> fieldTypes) throws SQLException {
        // Create the value map and list.  Both of these refer to the same value objects.  We
        // store the returned values into the list, and then they are accessible by name via the
        // map.
        final int n = fieldNames.size();
        this.valueList = fieldTypes.stream().map(x -> x.create()).collect(Collectors.toList());
        this.valueMap = new HashMap<>(n * 4 / 3);
        for (int i = 0; i < n; i++) {
            this.valueMap.put(fieldNames.get(i), this.valueList.get(i));
            this.valueList.get(i).fetch(results, i+1);
        }
    }

    /**
     * Find the specified field's holder and throw an error if it is not there.
     *
     * @param field		field specification (table.field)
     *
     * @return the holder for the named field
     *
     * @throws SQLException
     */
    private DbValue getField(String field) throws SQLException {
        DbValue value = this.valueMap.get(field);
        if (value == null)
            throw new SQLException("Field \"" + field + "\" is not present in this query.");
        return value;
    }

    /**
     * @return TRUE if the specified field is NULL, else FALSE
     *
     * @param field		field specification (table.field)
     *
     * @throws SQLException
     */
    public boolean isNull(String field) throws SQLException {
        DbValue value = this.getField(field);
        return value.isNull();
    }

    /**
     * @return the specified field value as a string
     *
     * @param field		field specification (table.field)
     *
     * @throws SQLException
     */
    public String getString(String field) throws SQLException {
        DbValue value = getField(field);
        return value.getString();
    }

    /**
     * @return the specified field value as a string, or as an empty string if it is NULL
     *
     * @param field		field specification (table.field)
     *
     * @throws SQLException
     */
    public String getReportString(String field) throws SQLException {
        String retVal = this.getString(field);
        if (retVal == null) retVal = "";
        return retVal;
    }

    /**
     * @return the specified field value as an integer
     *
     * @param field		field specification (table.field)
     *
     * @throws SQLException
     */
    public int getInt(String field) throws SQLException {
        DbValue value = this.getField(field);
        return value.getInt();
    }

    /**
     * @return the specified field value as an floating-point
     *
     * @param field		field specification (table.field)
     *
     * @throws SQLException
     */
    public double getDouble(String field) throws SQLException {
        DbValue value = this.getField(field);
        return value.getDouble();
    }

    /**
     * @return the specified field as a value object
     *
     * @param field		field specification (table.field)
     *
     * @throws SQLException
     */
    public DbValue getValue(String field) throws SQLException {
        return this.getField(field);
    }

    /**
     * @return the specified field value as a date
     *
     * @param field		field specification (table.field)
     *
     * @throws SQLException
     */
    public Instant getDate(String field) throws SQLException {
        DbValue value = this.getField(field);
        if (! DbDate.class.isAssignableFrom(value.getClass()))
            throw new SQLException("Field " + field + " is not a date.");
        return ((DbDate) value).get();
    }

    /**
     * @return the specified field value as a location
     *
     * @param field		field specification (table.field)
     *
     * @throws SQLException
     */
    public Location getLocation(String field) throws SQLException {
        DbValue value = this.getField(field);
        if (! DbLocation.class.isAssignableFrom(value.getClass()))
            throw new SQLException("Field " + field + " is not a date.");
        return ((DbLocation) value).get();
    }

    /**
     * @return the specified field value as a double array
     *
     * @param field		field specification (table.field)
     *
     * @throws SQLException
     */
    public double[] getDoubleArray(String field) throws SQLException {
        DbValue value = this.getField(field);
        if (! DbDoubleArray.class.isAssignableFrom(value.getClass()))
            throw new SQLException("Field " + field + " is not a date.");
        return ((DbDoubleArray) value).get();
    }

    /**
     * This retrieves the field as a boolean.  If the field has an integer value, then
     * nonzero is TRUE.  NULL or 0 is false.
     *
     * @param field		field specification (table.field)
     *
     * @return the specified field value as a boolean
     *
     * @throws SQLException
     */
    public boolean getBool(String field) throws SQLException {
        DbValue value = this.getField(field);
        boolean retVal = ! value.isNull();
        if (retVal)
            retVal = (value.getInt() != 0);
        return retVal;
    }




}
