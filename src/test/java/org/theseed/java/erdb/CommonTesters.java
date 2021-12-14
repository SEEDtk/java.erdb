/**
 *
 */
package org.theseed.java.erdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.java.erdb.types.DbLocation;
import org.theseed.locations.Location;

/**
 * This class contains methods that can be called to stress-test databases.  It assumes the
 * connection is already made, allowing the same tests to be run for multiple DB engines.
 *
 * @author Bruce Parrello
 *
 */
public class CommonTesters {

    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(CommonTesters.class);

    // static fields used
    private static final Location locPeg1 = Location.create("511145.1832:NODE_1_length_927033_cov_53.679905", 2000, 3000);
    private static final Location locPeg2 = Location.create("511145.1832:NODE_1_length_927033_cov_53.679905", 2500, 3600);

    /**
     * Test loading and querying a database.
     *
     * @param db	target database
     *
     * @throws SQLException
     * @throws IOException
     */
    public static void testLoading(DbConnection db) throws SQLException, IOException {
        DbTable rnaSampleTable = db.getTable("RnaSample");
        assertThat(rnaSampleTable, nullValue());
        db.scriptUpdate(new File("data", "rnaseqdb.sql"));
        try (DbConnection.Transaction xact = db.new Transaction()) {
            try (DbLoader loader = new DbLoader(db, "Genome")) {
                loader.set("genome_id", "511145.183");
                loader.set("genome_name", "Escherichia coli K-12 MG1655 test genome");
                loader.insert();
            }
            try (DbLoader loader = new DbLoader(db, "Feature")) {
                loader.set("fig_id", "fig|511145.183.peg.1");
                loader.set("genome_id", "511145.183");
                loader.setNull("alias");
                loader.setNull("gene_name");
                loader.set("seq_no", 1);
                loader.set("assignment", "hypothetical protein");
                loader.set("location", locPeg1);
                loader.insert();
                loader.set("fig_id", "fig|511145.183.peg.2");
                loader.set("genome_id", "511145.183");
                loader.set("gene_name", "thrA");
                loader.set("seq_no", 2);
                loader.set("assignment", "concrete protein with a long function");
                loader.set("location", locPeg2);
                loader.insert();
            }
            xact.commit();
        }
        // Now we have set up a genome and two features.  Verify by reading them back.
        try (DbQuery query = new DbQuery(db, "Genome1 Feature2")) {
            query.rel("Genome1.genome_id", Relop.EQ).orderBy("Feature2.seq_no");
            query.select("Genome1", "genome_id", "genome_name");
            query.selectAll("Feature2");
            query.setParm(1, "511145.183");
            Iterator<DbRecord> iter = query.iterator();
            assertThat("No records found.", iter.hasNext());
            DbRecord record = iter.next();
            assertThat(record.getString("Genome1.genome_id"), equalTo("511145.183"));
            assertThat(record.getString("Genome1.genome_name"), equalTo("Escherichia coli K-12 MG1655 test genome"));
            assertThat(record.getString("Feature2.fig_id"), equalTo("fig|511145.183.peg.1"));
            assertThat(record.getString("Feature2.genome_id"), equalTo("511145.183"));
            assertThat(record.getInt("Feature2.seq_no"), equalTo(1));
            assertThat(record.getString("Feature2.alias"), nullValue());
            assertThat(record.getString("Feature2.gene_name"), nullValue());
            assertThat(record.getString("Feature2.assignment"), equalTo("hypothetical protein"));
            Location loc = ((DbLocation) record.getValue("Feature2.location")).get();
            assertThat(loc, equalTo(locPeg1));
            assertThat("Only one record found.", iter.hasNext());
            record = iter.next();
            assertThat(record.getString("Genome1.genome_id"), equalTo("511145.183"));
            assertThat(record.getString("Genome1.genome_name"), equalTo("Escherichia coli K-12 MG1655 test genome"));
            assertThat(record.getString("Feature2.fig_id"), equalTo("fig|511145.183.peg.2"));
            assertThat(record.getString("Feature2.genome_id"), equalTo("511145.183"));
            assertThat(record.getInt("Feature2.seq_no"), equalTo(2));
            assertThat(record.getString("Feature2.alias"), nullValue());
            assertThat(record.getString("Feature2.gene_name"), equalTo("thrA"));
            assertThat(record.getString("Feature2.assignment"), equalTo("concrete protein with a long function"));
            loc = ((DbLocation) record.getValue("Feature2.location")).get();
            assertThat(loc, equalTo(locPeg2));
            assertThat("Too many records found.", ! iter.hasNext());
        }
    }

    // TODO date, double-array, null integer, double

}
