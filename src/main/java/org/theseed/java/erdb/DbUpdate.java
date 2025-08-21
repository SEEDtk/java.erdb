/**
 *
 */
package org.theseed.java.erdb;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

/**
 * This object manages an SQL UPDATE statement.  The updates are batched automatically, but the
 * capabilities are limited to an identity filter on one or more fields.  For more complicated updates,
 * use native JDBC statements without batching.
 *
 * @author Bruce Parrello
 *
 */
public class DbUpdate extends DbBaseUpdate {

    // FIELDS
    /** name of the filtering field */
    private Set<String> filterFields;
    /** list of fields to set (in order) */
    private Set<String> setFields;
    /** TRUE if we have created the statement */
    private boolean stmtCreated;

    /**
     * Create an update statement.
     *
     * @param db		database being updated
     * @param table		table being udpated
     * @param batchSize	number of updates per batch
     *
     * @throws SQLException
     */
    protected DbUpdate(DbConnection db, String table, int batchSize) throws SQLException {
        super(db, table, batchSize);
    }

    /**
     * Create an update without batching.
     *
     * @param db		database being updated
     * @param table		table being udpated
     *
     * @throws SQLException
     */
    public static DbUpdate single(DbConnection db, String table) throws SQLException {
        DbUpdate retVal = new DbUpdate(db, table, 1);
        retVal.initialize(db, table);
        return retVal;
    }

    /**
     * Create an update with batching.
     *
     * @param db		database being updated
     * @param table		table being udpated
     *
     * @throws SQLException
     */
    public static DbUpdate batch(DbConnection db, String table) throws SQLException {
        DbUpdate retVal = new DbUpdate(db, table, DbBaseUpdate.MAX_BATCH_SIZE);
        retVal.initialize(db, table);
        return retVal;
    }

    @Override
    protected void initCommand(DbConnection db, String table) throws SQLException {
        // Denote the statement is currently empty.
        this.filterFields = new TreeSet<>();
        this.setFields = new TreeSet<>();
        this.stmtCreated = false;
    }

    /**
     * Specify one or more fields to be updated be SET clauses.
     *
     * @param fields	names of fields to be changed
     *
     * @return this object, for fluent invocation
     */
    public DbUpdate change(String... fields) {
        this.setFields.addAll(Arrays.asList(fields));
        return this;
    }

    /**
     * Specify one or more fields to be used in WHERE clauses.  This method is additive.
     * That is, it adds to existing filters.
     *
     * @param fields	names of fields to be used for filtering
     *
     * @return this object, for fluent invocation
     */
    public DbUpdate filter(String... fields) {
        this.filterFields.addAll(Arrays.asList(fields));
        return this;
    }

    /**
     * Specify filtering by primary key.  This method erases all other filters.
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbUpdate primaryKey() throws SQLException {
        this.filterFields.clear();
        String keyName = this.getTableData().getKeyName();
        if (keyName == null)
            throw new SQLException("Cannot use primary key filtering on table " + this.getTableData().getName()
                    + ", which has no primary key.");
        this.filterFields.add(keyName);
        return this;
    }

    /**
     * Submit the current update.
     *
     * @throws SQLException
     */
    public void update() throws SQLException {
        if (! this.stmtCreated)
            throw new SQLException("Cannot do an update on an uncreated update statement.");
        // Execute the update.
        this.submit();
    }

    /**
     * Finalize the update statement for submitting updates.
     *
     * @throws SQLException
     */
    public void createStatement() throws SQLException {
        // Here we have to build the statement itself.
        DbTable tableDesc = this.getTableData();
        String table = tableDesc.getName();
        SqlBuffer buffer = new SqlBuffer(this.getDb()).append("UPDATE ").quote(table)
                .append(" SET ").startList();
        // Add all the fields being updated.
        for (String field : this.setFields) {
            buffer.appendDelim().quote(field).append(" = ").appendMark();
            this.addParm(field);
        }
        // Start the filter clause.
        if (! this.filterFields.isEmpty()) {
            buffer.append(" WHERE ").startList(" AND ");
            // Add all the filtering fields.
            for (String field : this.filterFields) {
                buffer.appendDelim().quote(field).append(" = ").appendMark();
                this.addParm(field);
            }
        }
        // Now create the statement.
        this.createStatement(buffer);
        this.stmtCreated = true;
    }


}
