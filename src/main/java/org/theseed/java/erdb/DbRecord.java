/**
 *
 */
package org.theseed.java.erdb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        this.valueMap = new HashMap<String, DbValue>(n * 4 / 3);
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
     * @return the specified field as a value object
     *
     * @param field		field specification (table.field)
     *
     * @throws SQLException
     */
    public DbValue getValue(String field) throws SQLException {
        return this.getField(field);
    }

}
