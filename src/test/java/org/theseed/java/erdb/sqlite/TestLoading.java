/**
 *
 */
package org.theseed.java.erdb.sqlite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbLoader;
import org.theseed.java.erdb.DbTable;
import org.theseed.locations.Location;

/**
 * @author Bruce Parrello
 *
 */
class TestLoading {

    @Test
    void testFeatureLoad() throws IOException, SQLException {
        File dbFile = new File("data", "temp.ser");
        if (dbFile.exists())
            FileUtils.forceDelete(dbFile);
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
                    loader.set("seq_no", 1);
                    loader.set("assignment", "hypothetical protein");
                    // TODO set for a DbType, create DbTypes for customs
                    //loader.set("location", Location.create("chrome1", 500, 400));
                }
            }
        }
    }

}
