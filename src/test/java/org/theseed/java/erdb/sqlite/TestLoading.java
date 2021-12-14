/**
 *
 */
package org.theseed.java.erdb.sqlite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbLoader;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.DbTable;
import org.theseed.java.erdb.Relop;
import org.theseed.java.erdb.types.DbLocation;
import org.theseed.locations.Location;

/**
 * @author Bruce Parrello
 *
 */
public class TestLoading {

    @Test
    public void testFeatureLoad() throws IOException, SQLException {
        File dbFile = new File("data", "temp.ser");
        if (dbFile.exists())
            FileUtils.forceDelete(dbFile);
        Location locPeg1 = Location.create("511145.1832:NODE_1_length_927033_cov_53.679905", 2000, 3000);
        Location locPeg2 = Location.create("511145.1832:NODE_1_length_927033_cov_53.679905", 2500, 3600);
        try (DbConnection db = new SqliteDbConnection(dbFile)) {
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
    }

    // TODO date, double-array, null integer, double

}
