/**
 *
 */
package org.theseed.java.erdb.sqlite;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.theseed.java.erdb.CommonTesters;
import org.theseed.java.erdb.DbConnection;

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
        try (DbConnection db = new SqliteDbConnection(dbFile)) {
            CommonTesters.testLoading(db);
        }
    }

}
