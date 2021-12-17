/**
 *
 */
package org.theseed.java.erdb.mysql;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbType;
import org.theseed.java.erdb.SqlBuffer;

/**
 * This object allows connections to a MySQL database.  In order to stay compatible with
 * SQLite, only a subset of operations and datatypes are supported.
 *
 * @author Bruce Parrello
 *
 */
public class MysqlDbConnection extends DbConnection {

    // FIELDS
    /** database name */
    private String dbName;

    /**
     * Connect to a MySQL database with the given URL.  Note that a username and password are
     * frequently required.
     *
     * @param dbUrl		database URL, consisting of the web host and the name (e.g. "localhost/rnaseq")
     * @param parms		the database property string, or NULL if there is none
     *
     * @throws SQLException
     */
    public MysqlDbConnection(String dbUrl, String parms) throws SQLException {
        String connectString = this.buildConnector(dbUrl, parms);
        this.connect(connectString);
    }

    /**
     * Connect to a MySQL database for a specified command processor.
     *
     * @param processor		controlling command processor
     *
     * @throws SQLException
     */
    public MysqlDbConnection(IParms processor) throws SQLException {
        String dbUrl = processor.getDbUrl();
        if (dbUrl == null)
            throw new SQLException("Database URL is required for MySQL.");
        String connectString = this.buildConnector(dbUrl, processor.getParms());
        this.connect(connectString);
    }

    /**
     * Build the connection string for this database from the URL and parameters.
     *
     * @param dbUrl		database URL (without protocol)
     * @param parms		database parameter string, or NULL for none
     *
     * @return the connection string
     */
    private String buildConnector(String dbUrl, String parms) {
        // Parse the name from the URL.
        this.dbName = StringUtils.substringAfterLast(dbUrl, "/");
        int len = 20 + dbUrl.length();
        if (parms != null) len += parms.length();
        StringBuilder retVal = new StringBuilder(len);
        retVal.append("jdbc:mysql://").append(dbUrl);
        if (parms != null)
            retVal.append("?").append(parms);
        return retVal.toString();
    }

    @Override
    protected Properties properties() {
        return null;
    }

    @Override
    public String getName() {
        return this.dbName;
    }

    @Override
    protected DbType parseType(String string) throws SQLException {
        DbType retVal = null;
        // Remove the length modifier.
        String primitive = StringUtils.substringBefore(string, "(").toUpperCase();
        switch (primitive) {
        case "VARCHAR" :
        case "CHAR" :
        case "TEXT" :
            retVal = DbType.STRING;
            break;
        case "BIT" :
        case "INT" :
        case "INTEGER" :
        case "SMALLINT" :
        case "TINYINT" :
        case "MEDIUMINT" :
            retVal = DbType.INTEGER;
            break;
        case "FLOAT" :
        case "DOUBLE" :
            retVal = DbType.DOUBLE;
            break;
        default :
            throw new SQLException("Unsupported field type " + primitive + ".");
        }
        return retVal;
    }

    @Override
    protected void quote(SqlBuffer queryBuffer, String name) {
        queryBuffer.append("`").append(name).append("`");

    }

    @Override
    protected String getCatalog() {
        return null;
    }

    @Override
    protected String getSchema() {
        return this.dbName;
    }

}
