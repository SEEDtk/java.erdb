/**
 *
 */
package org.theseed.java.erdb;

import java.sql.SQLException;

/**
 * This enumeration represents a database field type.
 *
 * @author Bruce Parrello
 *
 */
public enum DbType {
    INTEGER, DOUBLE, STRING, DOUBLE_ARRAY, DATE, BOOLEAN, LOCATION;

/**
 * @return the field type for the specified data type string
 *
 * @param db			parent database connection
 * @param string		data type string to parse
 */
    public static DbType parse(DbConnection db, String string) {
        return db.parseType(string);
    }

/**
 * @return the custom field type for the specified type name
 *
 * @param string		type name from the _fields meta-table
 *
 * @throws SQLException
 */
    public static DbType parse(String string) throws SQLException {
        DbType retVal;
        switch (string) {
        case "DOUBLE_ARRAY" :
            retVal = DOUBLE_ARRAY;
            break;
        case "DATE" :
            retVal = DATE;
            break;
        case "BOOLEAN" :
            retVal = BOOLEAN;
            break;
        case "LOCATION" :
            retVal = LOCATION;
            break;
        default:
            throw new SQLException("Invalid custom type \"" + string + "\".");
        }
        return retVal;
    }

}

