/**
 *
 */
package org.theseed.erdb.utils;

import org.theseed.utils.BaseProcessor;
import java.io.File;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.java.erdb.DbConnection;

/**
 * This is the base class for a database command processor.  It supports connecting to the
 * database and making it available to the subclass.
 *
 * The following command-line options are built-in.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 *
 * @author Bruce Parrello
 *
 */
public abstract class BaseDbProcessor extends BaseProcessor implements DbConnection.IParms {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(BaseDbProcessor.class);

    // COMMAND-LINE OPTIONS

    /** database engine type */
    @Option(name = "--type", usage = "type of database engine")
    private DbConnection.Type dbEngine;

    /** name of file containing the database */
    @Option(name = "--dbfile", metaVar = "sqlite.db", usage = "name of the database file (for SQLITE)")
    private File dbFile;


    @Override
    protected final void setDefaults() {
        this.dbEngine = DbConnection.Type.SQLITE;
        this.dbFile = null;
        this.setDbDefaults();
    }

    /**
     * Specify the defaults for the subclass options.
     */
    protected abstract void setDbDefaults();

    @Override
    protected final void runCommand() throws Exception {
        try (DbConnection db = this.dbEngine.create(this)) {
            this.runDbCommand(db);
        }
    }

    /**
     * Execute the command against the database.
     *
     * @param db	database to use for the command's data
     */
    protected abstract void runDbCommand(DbConnection db) throws Exception;

    @Override
    public File getDbFile() {
        return this.dbFile;
    }

}
