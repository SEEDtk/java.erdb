/**
 *
 */
package org.theseed.java.erdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.theseed.java.erdb.sqlite.SqliteDbConnection;

/**
 * @author Bruce Parrello
 *
 */
class TestSqlBuffer {

    @Test
    void testSqlBuffer() throws SQLException {
        File dbFile = new File("data", "chinook.db");
        try (DbConnection db = new SqliteDbConnection(dbFile)) {
            SqlBuffer buff1 = new SqlBuffer(db);
            assertThat("Buffer should be empty.", buff1.isEmpty());
            List<String> tables = db.getTableNames();
            Collections.sort(tables);
            for (String table : tables)
                buff1.appendDelim(", ").quote(table);
            assertThat(buff1.toString(),
                    equalTo("[albums], [artists], [customers], [employees], [genres]," +
                            " [invoice_items], [invoices], [media_types], [playlist_track]," +
                            " [playlists], [tracks]"));
            buff1.clear();
            assertThat("Buffer should be empty.", buff1.isEmpty());
            assertThat(buff1.toString(), equalTo(""));
            String[] fieldList = new String[] { "TrackId", "Name", "AlbumId", "Composer" };
            buff1.append("INSERT INTO ").quote("tracks").append(" ").addFields(fieldList);
            buff1.append(" VALUES ").addMarkList(4);
            assertThat(buff1.toString(),
                    equalTo("INSERT INTO [tracks] ([TrackId], [Name], [AlbumId], [Composer])" +
                            " VALUES (?,?,?,?)"));
            assertThat(buff1.getMarkCount(), equalTo(4));
            buff1.clear();
            buff1.quote("Genome", "genome_id");
            assertThat(buff1.toString(), equalTo("[Genome].[genome_id]"));

        }
    }

}
