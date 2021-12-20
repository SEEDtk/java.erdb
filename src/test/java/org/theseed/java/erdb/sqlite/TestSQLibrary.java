package org.theseed.java.erdb.sqlite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

public class TestSQLibrary {

    @Test
    public void testDatabaseCreate() throws SQLException, IOException {
        File dbFile = new File("data", "temp.ser");
        if (dbFile.exists())
            FileUtils.forceDelete(dbFile);
        try (Connection dbc = DriverManager.getConnection("jdbc:sqlite:" + dbFile.toString())) {
            assertThat(dbc, not(nullValue()));
            assertThat("database was not created", dbFile.exists());
            try (Statement stmt = dbc.createStatement()) {
                String sql = "CREATE TABLE COMPANY " +
                        "(ID INT PRIMARY KEY     NOT NULL," +
                        " NAME           TEXT    NOT NULL, " +
                        " AGE            INT     NOT NULL, " +
                        " ADDRESS        CHAR(50), " +
                        " SALARY         REAL)";
                stmt.executeUpdate(sql);
                sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) " +
                               "VALUES (1, 'Paul', 32, 'California', 20000.00 );";
                int rc = stmt.executeUpdate(sql);
                assertThat(rc, equalTo(1));
                sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) " +
                         "VALUES (2, 'Allen', 25, 'Texas', 15000.00 );";
                rc = stmt.executeUpdate(sql);
                assertThat(rc, equalTo(1));
                sql = "SELECT id, name FROM COMPANY ORDER BY ID";
                ResultSet results = stmt.executeQuery(sql);
                assertThat("query failed", ! results.isClosed());
                assertThat("no records", results.next());
                int id = results.getInt(1);
                String name = results.getString(2);
                assertThat(id, equalTo(1));
                assertThat(name, equalTo("Paul"));
                assertThat("no second record", results.next());
                id = results.getInt(1);
                name = results.getString(2);
                assertThat(id, equalTo(2));
                assertThat(name, equalTo("Allen"));
                assertThat("too many records", ! results.next());
            }
        }
    }

}
