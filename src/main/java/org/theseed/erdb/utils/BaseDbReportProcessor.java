/**
 *
 */
package org.theseed.erdb.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.kohsuke.args4j.Option;
import org.theseed.basic.ParseFailureException;
import org.theseed.java.erdb.DbConnection;

/**
 * This is a variant of a database command processor that produces a report to a flat file.  It supports
 * connecting to the database and making it available to the subclass, plus it opens a PrintWriter for
 * output.
 *
 * The following command-line options are built-in.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	output file (if not STDOUT)
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 *
 * @author Bruce Parrello
 *
 */
public abstract class BaseDbReportProcessor extends BaseDbProcessor {

    /** output stream */
    private OutputStream outStream;

    // COMMAND-LINE OPTIONS

    /** output file (if not STDOUT) */
    @Option(name = "-o", aliases = { "--output" }, usage = "output file for report (if not STDOUT)")
    private File outFile;


    @Override
    protected void setDbDefaults() {
        this.outFile = null;
        this.setReporterDefaults();
    }

    /**
     * Set the default values of command-line options.
     */
    protected abstract void setReporterDefaults();

    @Override
    protected final void validateParms() throws IOException, ParseFailureException {
        this.validateReporterParms();
        if (this.outFile == null) {
            log.info("Output will be to the standard output.");
            this.outStream = System.out;
        } else {
            log.info("Output will be to {}.", this.outFile);
            this.outStream = new FileOutputStream(this.outFile);
        }
    }

    /**
     * Validate the command-line options and parameters.
     *
     * @throws IOException
     * @throws ParseFailureException
     */
    protected abstract void validateReporterParms() throws IOException, ParseFailureException;


    @Override
    protected void runDbCommand(DbConnection db) throws Exception {
        try (PrintWriter writer = new PrintWriter(this.outStream)) {
            this.runDbReporter(db, writer);
        } finally {
            // Insure the output file is closed.
            if (this.outFile != null)
                this.outStream.close();
        }
    }

    /**
     * Create the report from the database.
     *
     * @param db		database from which to extract the data
     * @param writer	output writer for the report
     *
     * @throws Exception
     */
    protected abstract void runDbReporter(DbConnection db, PrintWriter writer) throws Exception;

}
