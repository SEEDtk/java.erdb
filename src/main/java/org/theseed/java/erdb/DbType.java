/**
 *
 */
package org.theseed.java.erdb;

import java.sql.SQLException;

import org.theseed.java.erdb.types.DbBoolean;
import org.theseed.java.erdb.types.DbDate;
import org.theseed.java.erdb.types.DbDouble;
import org.theseed.java.erdb.types.DbDoubleArray;
import org.theseed.java.erdb.types.DbInteger;
import org.theseed.java.erdb.types.DbLocation;
import org.theseed.java.erdb.types.DbString;

/**
 * This enumeration represents a database field type.
 *
 * @author Bruce Parrello
 *
 */
public enum DbType {
    INTEGER {

        @Override
        public DbValue create() {
            return new DbInteger();
        }

        @Override
        public boolean isComparable() {
            return true;
        }

    }, DOUBLE {

        @Override
        public DbValue create() {
            return new DbDouble();
        }

        @Override
        public boolean isComparable() {
            return true;
        }

    }, STRING {

        @Override
        public DbValue create() {
            return new DbString();
        }

        @Override
        public boolean isComparable() {
            return true;
        }

    }, DOUBLE_ARRAY {

        @Override
        public DbValue create() {
            return new DbDoubleArray();
        }

        @Override
        public boolean isComparable() {
            return false;
        }

    }, DATE {

        @Override
        public DbValue create() {
            return new DbDate();
        }

        @Override
        public boolean isComparable() {
            return true;
        }

    }, BOOLEAN {

        @Override
        public DbValue create() {
            return new DbBoolean();
        }

        @Override
        public boolean isComparable() {
            return true;
        }

    }, LOCATION {

        @Override
        public DbValue create() {
            return new DbLocation();
        }

        @Override
        public boolean isComparable() {
            return true;
        }

    };

    /**
     * @return a value object of the specified type
     */
    public abstract DbValue create();

    /**
     * @return TRUE if this type can be used in comparisons, else FALSE
     */
    public abstract boolean isComparable();

    /**
     * @return the field type for the specified data type string
     *
     * @param db			parent database connection
     * @param string		data type string to parse
     *
     * @throws SQLException
     */
    public static DbType parse(DbConnection db, String string) throws SQLException {
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

