/**
 *
 */
package org.theseed.java.erdb;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.LineReader;
import org.theseed.java.erdb.DbTable.FieldDesc;
import org.theseed.java.erdb.sqlite.SqliteDbConnection;

/**
 * This object manages an SQLite database connection.  It contains methods for passing in raw update
 * statements, loading tables, and creating queries.
 *
 * @author Bruce Parrello
 *
 */
public abstract class DbConnection implements AutoCloseable {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(DbConnection.class);
    /** connection to the database */
    private Connection db;
    /** metadata for the database */
    private DatabaseMetaData metaData;
    /** map of table names to table descriptors */
    private Map<String, DbTable> tableMap;
    /** special query for custom field meta-data */
    private PreparedStatement fieldTypeQuery;
    /** special query for placement data */
    private PreparedStatement placementQuery;
    /** queries to create field table */
    private static final String[] FIELD_CREATE = new String[] {
            "CREATE TABLE _fields (\n"
            + "	/* This contains the metadata for each database field with a special type or a comment */\n"
            + "	table_name VARCHAR(30) NOT NULL,\n"
            + "	field_name VARCHAR(30) NOT NULL,\n"
            + "	field_type VARCHAR(20) NOT NULL, /* type name in Java (INTEGER, DOUBLE, etc) */\n"
            + " description TEXT /* important notes about the field */\n"
            + "	);",
            "CREATE UNIQUE INDEX idx__fields ON _fields (table_name, field_name);"
        };
    private static final String[] DIAGRAM_CREATE = new String[] {
            "CREATE TABLE _diagram (\n"
            + "	/* This contains the metadata for each database table */\n"
            + "	table_name VARCHAR(30) PRIMARY KEY,\n"
            + "	rloc INTEGER NOT NULL, /* table's row location on the diagram */\n"
            + "	cloc INTEGER NOT NULL, /* table's column location on the diagram */\n"
            + " description TEXT NOT NULL /* description of the table */\n"
            + "	);"
        };
    /** array to search for metadata tables */
    private static final String[] TABLE_SEARCH = new String[] { "TABLE" };

    /**
     * This interface is used by the individual database connection services to get the database
     * connection information.
     *
     * @author Bruce Parrello
     *
     */
    public interface IParms {

        /**
         * @return the database file (for OBFF systems like SQLite)
         */
        public File getDbFile();

        /**
         * @return the database parameter string (often contains name and password)
         */
        public String getParms();

        /**
         * @return the database URL string (often contains host and name)
         */
        public String getDbUrl();

    }

    /**
     * This enum specified the database engine type.
     */
    public static enum Type {
        SQLITE {
            @Override
            public DbConnection create(IParms processor) throws SQLException {
                return new SqliteDbConnection(processor);
            }
        };

        /**
         * Connect to an existing database.
         *
         * @param processor		controlling command process
         *
         * @return a database connection object of this type
         *
         * @throws SQLException
         */
        public abstract DbConnection create(IParms processor) throws SQLException;

    }

    /**
     * This is a resource class that manages a transaction.  The transaction is rolled back if it is
     * not committed before it goes out of scope.
     */
    public class Transaction implements AutoCloseable {

        /** original value of autocommit */
        private boolean oldCommit;
        /** TRUE if the transaction was successful */
        private boolean committed;

        /**
         * Create the transaction.
         *
         * @throws SQLException
         */
        public Transaction() throws SQLException {
            this.oldCommit = DbConnection.this.db.getAutoCommit();
            DbConnection.this.db.setAutoCommit(false);
            this.committed = false;
        }

        /**
         * Indicate that the transaction was successful and should be committed.
         */
        public void commit() {
            this.committed = true;
        }

        @Override
        public void close() throws SQLException {
            if (this.committed)
                DbConnection.this.db.commit();
            else
                DbConnection.this.db.rollback();
            // Restore the auto-commit status.
            DbConnection.this.db.setAutoCommit(this.oldCommit);
        }

    }

    /**
     * Connect to the database using the specified connect string.
     *
     * @param connectString		database connection string
     *
     * @throws SQLException
     */
    protected void connect(String connectString) throws SQLException {
        this.db = DriverManager.getConnection(connectString, this.properties());
        log.info("Connected to database {}.", this.getName());
        this.metaData = this.db.getMetaData();
        // Create the table map.  It is initialized lazily: that is, we store each table
        // definition when the table is first used.
        this.tableMap = new TreeMap<String, DbTable>();
        // Insure the metadata tables exist.  We need a metadata query to get the list of all of them.
        ResultSet resultSet = this.metaData.getTables(this.getCatalog(), this.getSchema(), null, TABLE_SEARCH);
        Set<String> allTables = new TreeSet<String>();
        while (resultSet.next())
            allTables.add(resultSet.getString("TABLE_NAME"));
        if (! allTables.contains("_fields"))
            this.createMetaTable(FIELD_CREATE);
        if (! allTables.contains("_diagram"))
            this.createMetaTable(DIAGRAM_CREATE);
        // Create the metadata queries.
        this.prepareFieldTypeQuery();
        this.preparePlacementQuery();
    }

    /**
     * Create a meta-data table.
     *
     * @param sqlArray	array of SQL statements to execute
     *
     * @throws SQLException
     */
    private void createMetaTable(String[] sqlArray) throws SQLException {
        boolean oldCommit = db.getAutoCommit();
        try (Statement stmt = db.createStatement()) {
            this.db.setAutoCommit(false);
            for (String sql : sqlArray)
                stmt.execute(sql);
            this.db.commit();
        } finally {
            this.db.setAutoCommit(oldCommit);
        }
    }

    /**
     * Create the field-type query.
     *
     * @throws SQLException
     */
    private void prepareFieldTypeQuery() throws SQLException {
        this.fieldTypeQuery = this.db.prepareStatement(
                "SELECT field_name, field_type, description FROM _fields WHERE table_name = ?");
    }

    /**
     * Create the placement query.
     *
     * @throws SQLException
     */
    private void preparePlacementQuery() throws SQLException {
        this.placementQuery = this.db.prepareStatement(
                "SELECT rloc, cloc, description FROM _diagram WHERE table_name = ?");
    }

    /**
     * Get the placement metadata for a table.
     *
     * @param table		name of the desired table
     *
     * @return a result set for the query to get this table's placement metadata
     *
     * @throws SQLException
     */
    protected ResultSet loadPlacementRecord(String table) throws SQLException {
        this.placementQuery.setString(1, table);
        ResultSet retVal = this.placementQuery.executeQuery();
        return retVal;
    }


    /**
     * @return a map from field names to custom field information for the specified table
     *
     * @param table		name of the table whose custom-type fields are desired
     *
     * @throws SQLException
     */
    protected Map<String, DbTable.FieldDesc> getCustomTypes(String table) throws SQLException {
        var retVal = new TreeMap<String, FieldDesc>();
        this.fieldTypeQuery.setString(1, table);
        ResultSet results = this.fieldTypeQuery.executeQuery();
        while (results.next()) {
            String fieldName = results.getString("field_name");
            DbType fieldType = DbType.parse(results.getString("field_type"));
            String comment = results.getString("description");
            retVal.put(fieldName, new DbTable.FieldDesc(fieldType, comment));
        }
        return retVal;
    }

    /**
     * Read a batch of database updates from a file and run them.
     *
     * The input file should contain SQL statements delimited by semi-colons at the end of the
     * line.  If there are multiple statements on a line, or there are comments after the
     * semicolon, the process will fail.  This is, however, a fairly flexible format and
     * should be an easy convention to enforce.
     *
     * @param inFile	file of updates to read
     *
     * @throws SQLException
     * @throws IOException
     */
    public void scriptUpdate(File inFile) throws SQLException, IOException {
        Statement stmt = this.db.createStatement();
        StringBuilder buffer = new StringBuilder();
       try (LineReader sqlStream = new LineReader(inFile);
               Transaction xact = this.new Transaction()) {
            while (sqlStream.hasNext()) {
                String line = StringUtils.trim(sqlStream.next());
                buffer.append(line);
                // Check for end-of-statement.  If there is end-of-statement,  we add the SQL
                // and clear the buffer.  Otherwise, we add a space before the next line is
                // appended.
                if (line.endsWith(";")) {
                    stmt.addBatch(buffer.toString());
                    buffer.setLength(0);
                } else
                    buffer.append(" ");
            }
            // Execute the batch.
            stmt.executeBatch();
            // Commit the updates.
            xact.commit();
        } finally {
            stmt.close();
        }
    }

    /**
     * @return the property object for this database type (or NULL if none)
     */
    protected abstract Properties properties();

    /**
     * @return a list of table names in this database
     *
     * @throws SQLException
     */
    public List<String> getTableNames() throws SQLException {
        List<String> retVal = new ArrayList<String>();
        ResultSet results = this.metaData.getTables(this.getCatalog(), this.getSchema(), null, TABLE_SEARCH);
        while (results.next()) {
            String tableName = results.getString("TABLE_NAME");
            if (! tableName.startsWith("_"))
                retVal.add(tableName);
        }
        return retVal;
    }

    /**
     * Delete all of the tables from the database.
     *
     * @throws SQLException
     */
    public void clearTables() throws SQLException {
        // Create an SQL buffer for the drop statements.
        SqlBuffer buffer = new SqlBuffer(this);
        // This will be set to TRUE if we succeed.
        boolean done = false;
        // Get the table names.
        List<String> tables = this.getTableNames();
        // Turn off auto-commit.
        boolean oldCommit = this.db.getAutoCommit();
        this.db.setAutoCommit(false);
        // Create the statement.
        try (Statement stmt = this.db.createStatement()) {
            // We can't drop a table if it has imported keys from tables still in the database.
            // This set tracks the tables not yet dropped.
            Set<String> unDropped = new HashSet<String>(tables);
            // This map tells us the tables dependent on this one.
            Map<String, Set<String>> exportMap = new HashMap<String, Set<String>>(tables.size() * 4 / 3 + 1);
            String catalog = this.getCatalog();
            String schema = this.getSchema();
            for (String table : tables) {
                ResultSet exports = this.metaData.getExportedKeys(catalog, schema, table);
                Set<String> exportSet = new TreeSet<String>();
                while (exports.next()) {
                    String otherTable = exports.getString("FKTABLE_NAME");
                    if (! otherTable.contentEquals(table))
                        exportSet.add(otherTable);
                }
                exportMap.put(table, exportSet);
            }
            // Loop until all the tables are dropped.
            while (! unDropped.isEmpty()) {
                // Find the first table that is safe to drop.
                String table = null;
                Iterator<String> iter = unDropped.iterator();
                while (table == null && iter.hasNext()) {
                    // Get a candidate table.
                    String candidate = iter.next();
                    // Are all of its exports dropped?
                    boolean safe = exportMap.get(candidate).stream().allMatch(x -> ! unDropped.contains(x));
                    if (safe)
                        table = candidate;
                }
                if (table == null)
                    throw new SQLException("Cannot drop all tables: circular references found.");
                // Here it is safe to drop.
                buffer.start("DROP TABLE ").quote(table);
                unDropped.remove(table);
                stmt.execute(buffer.toString());
            }
            // Empty the meta-tables.
            buffer.start("DELETE FROM ").quote("_fields");
            stmt.execute(buffer.toString());
            buffer.start("DELETE FROM ").quote("_diagram");
            stmt.execute(buffer.toString());
            // Commit the updates.
            this.db.commit();
            // Erase the table map.
            this.tableMap.clear();
            // Denote this all worked.
            done = true;
        } finally {
            // If we did not commit, do a rollback.
            if (! done)
                this.db.rollback();
            // Restore the auto-commit state.
            this.db.setAutoCommit(oldCommit);
        }
    }

    @Override
    public void close() throws SQLException {
        // Clean up the special statements.
        if (this.fieldTypeQuery != null)
            this.fieldTypeQuery.close();
        // Close the database.
        this.db.close();
        log.info("Closed database {}.", this.getName());
    }

    /**
     * @return the name of this database
     */
    protected abstract String getName();


    /**
     * @return the database metaData
     */
    public DatabaseMetaData getMetaData() {
        return this.metaData;
    }

    /**
     * @return the field type for the specified type string
     *
     * @param string	type string to parse
     *
     * @throws SQLException
     */
    protected abstract DbType parseType(String string) throws SQLException;

    /**
     * Store a quoted string in the specified text buffer.
     *
     * @param queryBuffer	text buffer where an SQL statement is being built
     * @param name			name to quote
     */
    protected abstract void quote(SqlBuffer queryBuffer, String name);


    /**
     * @return the catalog containing the current database
     */
    protected abstract String getCatalog();

    /**
     * @return the schema name of the current database
     */
    protected abstract String getSchema();

    /**
     * @return the definition for the named table
     *
     * @param tName		name of the table whose definition is desired
     * @throws SQLException
     */
    public DbTable getTable(String tName) throws SQLException {
        DbTable retVal = this.tableMap.get(tName);
        if (retVal == null) {
            retVal = DbTable.load(this, tName);
            // If the table exists, cache it in the table map.
            if (retVal != null)
                this.tableMap.put(tName, retVal);
        }
        return retVal;
    }

    /**
     * @return a prepared statement based on the SQL buffer.
     *
     * @param buffer	SQL buffer containing the statement text (with parameter marks)
     *
     * @throws SQLException
     */
    public PreparedStatement createStatement(SqlBuffer buffer) throws SQLException {
        PreparedStatement retVal = this.db.prepareStatement(buffer.toString());
        return retVal;
    }


}
