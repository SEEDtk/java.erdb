/**
 *
 */
package org.theseed.java.erdb;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.java.erdb.types.DbBoolean;
import org.theseed.java.erdb.types.DbDate;
import org.theseed.java.erdb.types.DbDouble;
import org.theseed.java.erdb.types.DbDoubleArray;
import org.theseed.java.erdb.types.DbInteger;
import org.theseed.java.erdb.types.DbLocation;
import org.theseed.java.erdb.types.DbString;
import org.theseed.locations.Location;

/**
 * This command loads records for a single table into the database.  A prepared statement is built
 * that allows insertion of all fields in the record.  The fields can be updated by the various "set"
 * methods using the field name.
 *
 * The parameter value objects are kept in an array (for access by index number) and a map (for access by
 * field name).  The same value objects are in both structures.
 *
 * Parameter values are not cleared between inserts, so if a value is not set it will remain the same.
 * This is not guaranteed to always be the case, however.
 *
 * @author Bruce Parrello
 *
 */
public class DbLoader implements AutoCloseable {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(DbLoader.class);
    /** array of parameter value objects */
    private DbValue[] parms;
    /** map of field names to parameter value objects */
    private Map<String, DbValue> fieldMap;
    /** statement performing the load */
    private PreparedStatement stmt;
    /** size of current batch */
    private int batchCount;
    /** maximum batch size */
    private static final int MAX_BATCH_SIZE = 100;

    /**
     * Construct a loader for the specified table.
     *
     * @param db		database connection
     * @param table		name of table to load
     *
     * @throws SQLException
     */
    public DbLoader(DbConnection db, String table) throws SQLException {
        // Get the data for this table.
        DbTable tableData = db.getTable(table);
        Collection<DbTable.Field> fields = tableData.getFields();
        // Create an array for the field names.
        final int nFields = fields.size();
        String[] fieldList = new String[nFields];
        // Create the parm array and map.
        this.parms = new DbValue[nFields];
        this.fieldMap = new HashMap<String, DbValue>(nFields * 4 / 3);
        // Fill all these in from the field list.
        int i = 0;
        for (DbTable.Field field : fields) {
            fieldList[i] = field.getName();
            this.parms[i] = field.getType().create();
            this.fieldMap.put(fieldList[i], this.parms[i]);
            i++;
        }
        // Now we have all the parameter holders created and connected to the field names.
        // Build the query itself.
        SqlBuffer buffer = (new SqlBuffer(db)).append("INSERT INTO ").quote(table).append(" ")
                .addFields(fieldList).append(" VALUES ").addMarkList(nFields);
        this.stmt = db.createStatement(buffer);
        // Denote we have an empty batch.
        this.batchCount = 0;
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
     * Insert the current record into the database.
     *
     * @throws SQLException
     */
    public void insert() throws SQLException {
        // Store the current parameter values in the statement.
        for (int i = 0; i < this.parms.length; i++) {
            this.parms[i].store(this.stmt, i+1);
        }
        // Add the statement with the current parameter values to the batch.
        this.stmt.addBatch();
        this.batchCount++;
        // If the batch is full, clear it.
        if (this.batchCount >= MAX_BATCH_SIZE)
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
        DbValue retVal = this.fieldMap.get(field);
        if (retVal == null)
            throw new SQLException("Field " + field + " does not exist in query.");
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

}
