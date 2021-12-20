/**
 *
 */
package org.theseed.java.erdb.mysql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.theseed.java.erdb.CommonTesters;

/**
 * To run this test you must have a MySQL database "rnaseq" on localhost with a user named "tester"
 * and a password "poobah".
 *
 * @author Bruce Parrello
 *
 */
public class TestMysqlConnection {

    @Test
    public void testMysql() throws SQLException, IOException {
        // We connect to the database and run the stress test in TestLoading.
        try (MysqlDbConnection db = new MysqlDbConnection("localhost/rnaseq", "user=tester&password=poobah")) {
            assertThat(db, not(nullValue()));
            db.clearTables();
            CommonTesters.testLoading(db);
        }
    }
}
