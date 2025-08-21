/**
 *
 */
package org.theseed.java.erdb;

import java.sql.SQLException;
import java.util.Collection;

/**
 * This class loads records for a single table into the database.  A prepared statement is built
 * that allows insertion of all fields in the record.  The fields can be updated by the various "set"
 * methods using the field name.
 *
 * The parameter value objects are kept in an array (for access by index number) and a map (for access by
 * field name).  The same value objects are in both structures.
 *
 * Parameter values are not cleared between inserts, so if a value is not set it will remain the same.
 *
 * @author Bruce Parrello
 *
 */
public class DbLoader extends DbBaseUpdate {

    /**
     * Construct a loader for the specified table.
     *
     * @param db		database connection
     * @param table		name of table to load
     * @param batchSize	batch size for updates
     *
     * @throws SQLException
     */
    protected DbLoader(DbConnection db, String table, int batchSize) throws SQLException {
        super(db, table, batchSize);
    }

    /**
     * Construct a batched loader for the specified table.
     *
     * @param db		database connection
     * @param table		name of table to load
     *
     * @throws SQLException
     */
    public static DbLoader batch(DbConnection db, String table) throws SQLException {
        DbLoader retVal = new DbLoader(db, table, MAX_BATCH_SIZE);
        retVal.initialize(db, table);
        return retVal;
    }

    /**
     * Construct a one-at-a-time loader for the specified table.
     *
     * @param db		database connection
     * @param table		name of table to load
     *
     * @throws SQLException
     */
    public static DbLoader single(DbConnection db, String table) throws SQLException {
        DbLoader retVal = new DbLoader(db, table, 1);
        return retVal;
    }

    /**
     * Initialize a table loader.
     *
     * @param db		database containing table
     * @param table		table being loaded
     *
     * @throws SQLException
     */
    @Override
    protected void initCommand(DbConnection db, String table) throws SQLException {
        // Get the data for this table.
        DbTable tableData = this.getTableData();
        Collection<DbTable.Field> fields = tableData.getFields();
        // Create an array for the field names.
        final int nFields = fields.size();
        String[] fieldList = new String[nFields];
        // Each field is added as a parameter.
        int i = 0;
        for (DbTable.Field field : fields) {
            fieldList[i] = field.getName();
            this.addParm(fieldList[i]);
            i++;
        }
        // Now we have all the parameter holders created and connected to the field names.
        // Build the query itself.
        SqlBuffer buffer = (new SqlBuffer(db)).append("INSERT INTO ").quote(table).append(" ")
                .addFields(fieldList).append(" VALUES ").addMarkList(nFields);
        this.createStatement(buffer);
    }

    /**
     * Insert the current record into the database.
     *
     * @throws SQLException
     */
    public void insert() throws SQLException {
        this.submit();
    }

}
