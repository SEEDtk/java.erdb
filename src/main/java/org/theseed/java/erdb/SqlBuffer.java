/**
 *
 */
package org.theseed.java.erdb;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

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
    /** TRUE if we are at the start of a list, else FALSE */
    private boolean starting;

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
        this.starting = true;
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
     * Append a delimiter to the SQL statement if the statement is non-empty.
     *
     * @param delim		delimiter to append
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer appendDelim(String delim) {
        if (this.buffer.length() > 0)
            this.buffer.append(delim);
        return this;
    }

    /**
     * @return TRUE if the buffer is empty, else FALSE
     */
    public boolean isEmpty() {
        return (this.buffer.length() <= 0);
    }

    /**
     * Reset the buffer for another command.
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer clear() {
        this.buffer.setLength(0);
        this.markCount = 0;
        this.starting = true;
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
            this.buffer.append(",?");
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
        this.append("(").quote(names[0]);
        for (int i = 1; i < names.length; i++)
            this.append(", ").quote(names[i]);
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

    /**
     * Append the contents of another SQL buffer to this one.
     *
     * @param other		other buffer to append
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer append(SqlBuffer other) {
        this.buffer.append(other.buffer);
        return this;
    }

    /**
     * Append a parameter mark.
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer appendMark() {
        this.buffer.append("?");
        this.markCount++;
        return this;
    }

    /**
     * Quote a field specification.  This consists of a table name, a dot, and a field name.
     *
     * @param fieldSpec		field specification to quote
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer quoteSpec(String fieldSpec) {
        String[] parts = StringUtils.split(fieldSpec, ".", 2);
        this.quote(parts[0], parts[1]);
        return this;
    }

    /**
     * Start a new SQL statement in this buffer.
     *
     * @param string	initial string to put into the buffer
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer start(String string) {
        this.clear();
        return this.append(string);
    }

    /**
     * Add a list of fields from a single table to this query, comma-delimited.
     *
     * @param table		target table
     * @param names		list of field names
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer quote(String table, List<String> names) {
        this.quote(table, names.get(0));
        final int n = names.size();
        for (int i = 1; i < n; i++)
            this.append(", ").quote(table, names.get(i));
        return this;
    }

    /**
     * Denote we are starting a list.  The default delimiter is suppressed in this mode.
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer startList() {
        this.starting = true;
        return this;
    }

    /**
     * Append the default delimiter.  If we are starting a list, nothing is appended.
     *
     * @return this object, for fluent invocation
     */
    public SqlBuffer appendDelim() {
        if (this.starting)
            this.starting = false;
        else
            this.append(", ");
        return this;
    }


}
