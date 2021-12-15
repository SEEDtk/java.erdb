/**
 *
 */
package org.theseed.java.erdb.sqlite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import java.io.File;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbTable;
import org.theseed.java.erdb.DbType;

/**
 * @author Bruce Parrello
 *
 */
public class TestDbConnection {

    @Test
    public void testTable() throws SQLException {
        DbConnection db = new SqliteDbConnection(new File("data", "chinook.db"));
        DbTable tracks = db.getTable("tracks");
        assertThat(tracks.getType("TrackId"), equalTo(DbType.INTEGER));
        assertThat(tracks.getType("Name"), equalTo(DbType.STRING));
        assertThat(tracks.getType("AlbumId"), equalTo(DbType.INTEGER));
        assertThat(tracks.getType("MediaTypeId"), equalTo(DbType.INTEGER));
        assertThat(tracks.getType("GenreId"), equalTo(DbType.INTEGER));
        assertThat(tracks.getType("Composer"), equalTo(DbType.STRING));
        assertThat(tracks.getType("Milliseconds"), equalTo(DbType.INTEGER));
        assertThat(tracks.getType("Bytes"), equalTo(DbType.INTEGER));
        assertThat(tracks.getType("UnitPrice"), equalTo(DbType.DOUBLE));
        assertThat("TrackId is nullable", ! tracks.isNullable("TrackId"));
        assertThat("Name is nullable", ! tracks.isNullable("Name"));
        assertThat("AlbumId is not nullable", tracks.isNullable("AlbumId"));
        assertThat("MediaTypeId is nullable", ! tracks.isNullable("MediaTypeId"));
        assertThat("GenreId is not nullable", tracks.isNullable("GenreId"));
        assertThat("Composer is not nullable", tracks.isNullable("Composer"));
        assertThat("Milliseconds is nullable", ! tracks.isNullable("Milliseconds"));
        assertThat("Bytes is not nullable", tracks.isNullable("Bytes"));
        assertThat("UnitPrice is nullable", ! tracks.isNullable("UnitPrice"));
        assertThat(tracks.getLink("artists"), nullValue());
    }

    @Test
    public void testMetaData() throws SQLException {
        DbConnection db = new SqliteDbConnection(new File("data", "chinook.db"));
        DatabaseMetaData meta = db.getMetaData();
        assertThat("Does not support batching.", meta.supportsBatchUpdates());
    }

    @Test void testDbLoad() throws SQLException, IOException {
        File dbFile = new File("data", "temp.ser");
        if (dbFile.exists())
            FileUtils.forceDelete(dbFile);
        try (DbConnection db = new SqliteDbConnection(dbFile)) {
            DbTable rnaSampleTable = db.getTable("RnaSample");
            assertThat(rnaSampleTable, nullValue());
            db.scriptUpdate(new File("data", "rnaseqdb.sql"));
            rnaSampleTable = db.getTable("RnaSample");
            assertThat(rnaSampleTable.getName(), equalTo("RnaSample"));
            Collection<DbTable.Field> fields = rnaSampleTable.getFields();
            assertThat(fields.size(), equalTo(12));
            assertThat(rnaSampleTable.getKeyName(), equalTo("sample_id"));
            assertThat(rnaSampleTable.getType("genome_id"), equalTo(DbType.STRING));
            assertThat(rnaSampleTable.getType("process_date"), equalTo(DbType.DATE));
            assertThat(rnaSampleTable.getType("read_count"), equalTo(DbType.INTEGER));
            assertThat("base_count is nullable", ! rnaSampleTable.isNullable("base_count"));
            assertThat(rnaSampleTable.getType("feat_data"), equalTo(DbType.DOUBLE_ARRAY));
            assertThat(rnaSampleTable.getType("suspicious"), equalTo(DbType.BOOLEAN));
            DbTable.Field pubmed = rnaSampleTable.getField("pubmed");
            assertThat("pubmed is not nullable", pubmed.isNullable());
            assertThat(pubmed.getComment(), equalTo("pubmed ID number for the paper relating to this sample (if any)"));
            DbTable.Field genomeId = rnaSampleTable.getField("genome_id");
            assertThat(genomeId.getComment(), not(nullValue()));
            DbTable.Placement placement = rnaSampleTable.getPlacement();
            assertThat(placement.getRow(), equalTo(3));
            assertThat(placement.getCol(), equalTo(3));
            assertThat(placement.getComment(), equalTo("This table contains the data for a single sample.  A sample always belongs to one genome.  The expression data is stored in an array of doubles (BLOB), with missing values stored as NaN.  The Feature table contains the indices for finding individual features in the array.  This limits our query ability, but greatly improves performance."));
            List<String> tables = db.getTableNames();
            assertThat(tables, containsInAnyOrder("FeatureGroup", "Genome", "Feature", "FeatureToGroup", "RnaSample",
                    "SampleCluster", "Measurement"));
            // Delete all the tables.
            db.clearTables();
            assertThat(db.getTableNames().size(), equalTo(0));
            assertThat(db.getTable("RnaSample"), nullValue());
        }
    }

}
