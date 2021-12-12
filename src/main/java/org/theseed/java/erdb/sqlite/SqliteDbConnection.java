/**
 *
 */
package org.theseed.java.erdb.sqlite;

import java.io.File;
import java.sql.SQLException;
import java.util.Properties;

import org.sqlite.SQLiteConfig;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbType;
import org.theseed.java.erdb.SqlBuffer;

/**
 * This is a database connection object for an SQLite database.  There are only three real
 * types, integer, string, and text.  We do not support blobs.  There is no catalog or
 * schema to worry about.  Quoting is done with square brackets.
 *
 * @author Bruce Parrello
 *
 */
public class SqliteDbConnection extends DbConnection {

    // FIELDS
    /** database file */
    private File dbFile;

    /**
     * Create a database connection to an SQLite database.
     *
     * @param dbFile	file containing the database
     *
     * @throws SQLException
     */
    public SqliteDbConnection(File dbFile) throws SQLException {
        this.dbFile = dbFile;
        setup();
    }

    /**
     * Setup the connection.
     *
     * @throws SQLException
     */
    private void setup() throws SQLException {
        this.connect("jdbc:sqlite:" + this.dbFile.getAbsolutePath());
    }

    /**
     * Construct a database connection to an SQLite database for a command processor.
     *
     * @param processor		controlling command processor
     *
     * @throws SQLException
     */
    public SqliteDbConnection(IParms processor) throws SQLException {
        this.dbFile = processor.getDbFile();
        if (this.dbFile == null)
            throw new SQLException("Database file is required for SQLITE databases.");
        this.setup();
    }

    @Override
    protected Properties properties() {
        SQLiteConfig config = new SQLiteConfig();
        config.enforceForeignKeys(true);
        return config.toProperties();
    }

    @Override
    protected String getName() {
        return this.dbFile.getAbsolutePath();
    }

    @Override
    protected DbType parseType(String string) {
        DbType retVal;
        if (string.contains("INT"))
            retVal = DbType.INTEGER;
        else if (string.contains("CHAR") || string.contains("CLOB") || string.contains("TEXT"))
            retVal = DbType.STRING;
        else if (string.contains("BOOLEAN"))
            retVal = DbType.INTEGER;
        else if (string.contains("BLOB"))
            retVal = DbType.DOUBLE_ARRAY;
        else
            retVal = DbType.DOUBLE;
        return retVal;
    }

    @Override
    protected void quote(SqlBuffer queryBuffer, String name) {
        queryBuffer.append("[").append(name).append("]");
    }

    @Override
    protected String getCatalog() {
        return null;
    }

    @Override
    protected String getSchema() {
        return null;
    }

}
