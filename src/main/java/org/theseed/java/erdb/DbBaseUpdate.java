/**
 *
 */
package org.theseed.java.erdb;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.theseed.java.erdb.types.DbBoolean;
import org.theseed.java.erdb.types.DbDate;
import org.theseed.java.erdb.types.DbDouble;
import org.theseed.java.erdb.types.DbDoubleArray;
import org.theseed.java.erdb.types.DbInteger;
import org.theseed.java.erdb.types.DbLocation;
import org.theseed.java.erdb.types.DbString;
import org.theseed.locations.Location;

/**
 * This class processes an update command in batches.  It is the base class for "DbLoader", which handles
 * the special case of insertions, and "DbUpdate", which handles the special case of updates.
 *
 * All the parameters are based on fields in the original record.  A particular field can only be used once.
 * This considerably limits the power of the WHERE clause in the SQL update; however, most such updates have a
 * pretty standard primarykey = value WHERE.
 *
 * The parameter value objects are kept in an array (for access by index number) and a map (for access by
 * field name).  The same value objects are in both structures.
 *
 * Parameter values are not cleared between individual updates, so if a value is not set it will remain the same.
 *
 * @author Bruce Parrello
 *
 */
public abstract class DbBaseUpdate implements AutoCloseable {

    // FIELDS
    /** parent database connection */
    private DbConnection db;
    /** array of parameter value objects */
    private List<DbValue> parms;
    /** map of field names to parameter positions */
    private Map<String, Integer> fieldMap;
    /** statement performing the update */
    private PreparedStatement stmt;
    /** size of current batch */
    private int batchCount;
    /** batch size to use */
    private final int batchSize;
    /** table field descriptor */
    private DbTable tableData;
    /** maximum batch size */
    protected static final int MAX_BATCH_SIZE = 100;

    /**
     * Construct an updater for the specified table.
     *
     * @param db		database connection
     * @param table		name of table to load
     * @param batchSize	batch size for updates
     *
     * @throws SQLException
     */
    protected DbBaseUpdate(DbConnection db, String table, int batchSize) throws SQLException {
        this.init(db, table);
        this.batchSize = batchSize;
    }

    /**
     * Call this method after construction to complete initialization.
     *
     * @param db        database connection
     * @param table     name of table to load
     * @throws SQLException
     */
    protected void initialize(DbConnection db, String table) throws SQLException {
        this.initCommand(db, table);
    }

    /**
     * Initialize the update command.
     *
     * @param db		relevant database connection
     * @param table		table being updated
     *
     * @throws SQLException
     */
    protected abstract void initCommand(DbConnection db, String table) throws SQLException;

    /**
     * Initialize a table loader.
     *
     * @param db		database containing table
     * @param table		table being loaded
     *
     * @throws SQLException
     */
    private void init(DbConnection db, String table) throws SQLException {
        // Save the database connection.
        this.db = db;
        // Get the table descriptor.
        this.tableData = db.getTable(table);
        // Get the number of fields so we can allocate memory.
        final int nFields = this.tableData.getFields().size();
        // Create the parm array and map.
        this.parms = new ArrayList<>(nFields);
        this.fieldMap = new HashMap<>(nFields * 4 / 3);
        // Denote we have an empty batch and no statement.
        this.batchCount = 0;
        this.stmt = null;
    }

    /**
     * Execute the current batch and denote that it is empty.
     *
     *  @throws SQLException
     */
    private void executeBatch() throws SQLException {
        this.stmt.executeBatch();
        this.batchCount = 0;
    }

    /**
     * Process the current update.
     *
     * @throws SQLException
     */
    protected void submit() throws SQLException {
        // Store the current parameter values in the statement.
        final int n = this.parms.size();
        for (int i = 0; i < n; i++) {
            this.parms.get(i).store(this.stmt, i+1);
        }
        // Add the statement with the current parameter values to the batch.
        this.stmt.addBatch();
        this.batchCount++;
        // If the batch is full, clear it.
        if (this.batchCount >= this.batchSize)
            this.executeBatch();
    }

    @Override
    public void close() throws SQLException {
        // If we have a batch, execute it.
        if (this.batchCount > 0)
            this.executeBatch();
        // If we have a statement, close that.
        if (this.stmt != null)
            this.stmt.close();
    }

    /**
     * Return the value holder for the specified field.
     *
     * @param field		name of the field of interest
     * @param class1	holder type for the field of interest
     *
     * @return the holder for the named field
     *
     * @throws SQLException
     */
    private DbValue getField(String field, Class<? extends DbValue> class1) throws SQLException {
        Integer pos = this.fieldMap.get(field);
        if (pos == null)
            throw new SQLException("Field " + field + " does not exist in statement.");
        DbValue retVal = this.parms.get(pos);
        if (! class1.isAssignableFrom(retVal.getClass()))
            throw new SQLException("Field " + field + " in query is not a " + class1.toString() + ".");
        return retVal;
    }

    /**
     * Store a string in the value holder for the specified field.
     *
     * @param field		name of the field
     * @param value		string value to store
     *
     * @throws SQLException
     */
    public void set(String field, String value) throws SQLException {
        // Update a string field.
        DbString stringHolder = (DbString) this.getField(field, DbString.class);
        stringHolder.set(value);
    }

    /**
     * Store an integer in the value holder for the specified field.
     *
     * @param field		name of the field
     * @param value		integer value to store
     *
     * @throws SQLException
     */
    public void set(String field, int value) throws SQLException {
        DbInteger intHolder = (DbInteger) this.getField(field, DbInteger.class);
        intHolder.set(value);
    }

    /**
     * Store a location in the value holder for the specified field.
     *
     * @param field		name of the field
     * @param loc		location value to store
     *
     * @throws SQLException
     */
    public void set(String field, Location loc) throws SQLException {
        DbLocation locHolder = (DbLocation) this.getField(field, DbLocation.class);
        locHolder.set(loc);
    }

    /**
     * Store a date in the value holder for the specified field.
     *
     * @param field		name of the field
     * @param date		date to store
     *
     * @throws SQLException
     */
    public void set(String field, Instant date) throws SQLException {
        DbDate dateHolder = (DbDate) this.getField(field, DbDate.class);
        dateHolder.set(date);
    }

    /**
     * Store a local date in the value holder for the specified field.
     *
     * @param field		name of the field
     * @param date		date to store
     *
     * @throws SQLException
     */
    public void set(String field, LocalDate date) throws SQLException {
        DbDate dateHolder = (DbDate) this.getField(field, DbDate.class);
        dateHolder.set(date);
    }

    /**
     * Store a double array in the value holder for the specified field.
     *
     * @param field		name of the field
     * @param array		double-array to store
     *
     * @throws SQLException
     */
    public void set(String field, double[] array) throws SQLException {
        DbDoubleArray arrayHolder = (DbDoubleArray) this.getField(field, DbDoubleArray.class);
        arrayHolder.set(array);
    }

    /**
     * Store a floating-point number in the value holder for the specified field.
     *
     * @param field		name of the field
     * @param value		value to store
     *
     * @throws SQLException
     */
    public void set(String field, double value) throws SQLException {
        DbDouble doubleHolder = (DbDouble) this.getField(field, DbDouble.class);
        doubleHolder.set(value);
    }

    /**
     * Store a null value in the value holder for the specified field.
     *
     * @param field		name of the field
     *
     * @throws SQLException
     */
    public void setNull(String field) throws SQLException {
        DbValue nullHolder = this.getField(field, DbValue.class);
        nullHolder.setNull();
    }

    /**
     * Store a boolean value in the value holder for the specified field.
     *
     * @param field		name of the field
     * @param b			boolean value to store
     *
     * @throws SQLException
     */
    public void set(String field, boolean b) throws SQLException {
        DbBoolean boolHolder = (DbBoolean) this.getField(field, DbBoolean.class);
        boolHolder.set(b);
    }

    /**
     * Add a parameter to this update statement.
     *
     * @param fieldName		name of field relevant to the parameter
     */
    protected void addParm(String fieldName) throws SQLException {
        DbTable.Field fieldDesc = this.tableData.getField(fieldName);
        // Get the parameter index of this new field.
        int idx = this.parms.size();
        // Now that we have the field descriptor, add a parameter holder of that
        // field's type and connect it to the field name.
        DbValue holder = fieldDesc.getType().create();
        this.parms.add(holder);
        this.fieldMap.put(fieldName, idx);
    }

    /**
     * Create the statement from the specified SQL statement buffer.
     *
     * @param buffer	SQL statement buffer containing the completed update command
     *
     * @throws SQLException
     */
    protected void createStatement(SqlBuffer buffer) throws SQLException {
        this.stmt = this.getDb().createStatement(buffer);
    }

    /**
     * @return the table descriptor
     */
    protected DbTable getTableData() {
        return this.tableData;
    }

    /**
     * @return the database connection
     */
    public DbConnection getDb() {
        return db;
    }

}
