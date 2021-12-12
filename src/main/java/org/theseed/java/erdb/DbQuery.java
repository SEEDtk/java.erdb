/**
 *
 */
package org.theseed.java.erdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This object holds a prepared statement for a database query.  The constructor specifies the path
 * through the database.  The user then adds fields and conditions using a fluent interface.
 * To execute the statement, the parameter marks must be filled in with actual values.  Each
 * record returned by the query is a map of field names to values.
 *
 * Field names are specified using a dot notation-- "XXX.YYY", where "XXX" is the table name and "YYY"
 * is the field name.
 *
 * The path through the database is specified using space-delimited table names.  A ">" delimiter
 * denotes a left join.  If a table is specified more than once, an alias will be generated using
 * the table name and a numeric suffix (e.g. "2", "3", etc.)
 *
 *
 *
 * @author Bruce Parrello
 *
 */
public class DbQuery {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(DbQuery.class);
    /** path through the database */
    private String[] path;

    // TODO data members for DbQuery

    // TODO constructors and methods for DbQuery
}
