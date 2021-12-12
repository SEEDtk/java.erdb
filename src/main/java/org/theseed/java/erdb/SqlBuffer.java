/**
 *
 */
package org.theseed.java.erdb;

/**
 * This object is an extension of a string builder that contains special methods for creating
 * SQL statements in a database-independent way.
 *
 * @author Bruce Parrello
 */
public class SqlBuffer {

    /** parent database connection */
    private DbConnection db;
    /** default buffer length */
    private static final int DEFAULT_LEN = 100;
    /** buffer for building the SQL statement */
    private StringBuilder buffer;
    /** number of parameter marks */
    private int markCount;

    /**
     * Create a new, empty query buffer for a database.
     *
     * @param db	database to be queried
     * @param init	initial capacity
     */
    public SqlBuffer(DbConnection db, int init) {
        this.setup(db, init);
    }

    /**
     * Initialize this object.
     *
     * @param db		parent database
     * @param init		initial buffer capacity
     */
    private void setup(DbConnection db, int init) {
        this.buffer = new StringBuilder(init);
        this.db = db;
        this.markCount = 0;
    }

    /**
     * Create a new, empty query buffer for a database.
     *
     * @param db	database to be queried
     */
    public SqlBuffer(DbConnection db) {
        this.setup(db, DEFAULT_LEN);
    }

    /**
     * Store a quoted field or table name in the query buffer.
     *
     * @param name		name to quote
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer quote(String name) {
        this.db.quote(this, name);
        return this;
    }

    /**
     * Store a qualified field name in the querry buffer.
     *
     * @param tName		table name to quote
     * @param fName		field name to quote
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer quote(String tName, String fName) {
        this.db.quote(this, tName);
        this.append(".");
        this.db.quote(this, fName);
        return this;
    }

    /**
     * Append a string to the SQL statement.
     *
     * @param string	string to append
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer append(String string) {
        this.buffer.append(string);
        return this;
    }

    /**
     * Reset the buffer for another command.
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer clear() {
        this.buffer.setLength(0);
        this.markCount = 0;
        return this;
    }

    /**
     * Add a set of parameter marks enclosed in parentheses.
     *
     * @param count		number of parameter marks to add
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer addMarkList(int count) {
        if (count <= 0)
            throw new IllegalArgumentException("Mark count cannot be 0.");
        this.buffer.append("(?");
        for (int i = 1; i < count; i++)
            this.buffer.append(", ?");
        this.buffer.append(")");
        this.markCount += count;
        return this;
    }

    /**
     * Add a set of column names enclosed in parentheses.
     *
     * @param names	array of column names
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer addFields(String[] names) {
        if (names.length <= 0)
            throw new IllegalArgumentException("Field count cannot be 0.");
        this.buffer.append('(').append(names[0]);
        for (int i = 1; i < names.length; i++)
            this.buffer.append(", ").append(names[i]);
        this.buffer.append(')');
        return this;
    }

    @Override
    public String toString() {
        return this.buffer.toString();
    }

    /**
     * @return the number of parameter marks in this statement
     */
    public int getMarkCount() {
        return this.markCount;
    }



}
