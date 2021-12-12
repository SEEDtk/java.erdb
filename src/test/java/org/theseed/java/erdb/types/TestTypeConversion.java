/**
 *
 */
package org.theseed.java.erdb.types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import java.io.File;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.theseed.java.erdb.DoubleInputStream;

/**
 * @author Bruce Parrello
 *
 */
class TestTypeConversion {

    @Test
    void testBlobConversion() throws SQLException, IOException {
        // We need a connection to create blobs.
        File dbFile = new File("data", "temp.ser");
        if (dbFile.exists())
            FileUtils.forceDelete(dbFile);
        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getAbsolutePath());
        Blob blob = conn.createBlob();
        // Fill the blob from a double array.
        double[] doubleArray = new double[] { 1.0, 2.2, 0.3, 0.04, 555.555 };
        try (DoubleInputStream doubleStream = new DoubleInputStream(doubleArray)) {
            byte[] byteArray = doubleStream.readAllBytes();
            blob.setBytes(0, byteArray);
        }
        double[] testArray = DbDoubleArray.blobToArray(blob);
        assertThat(doubleArray, equalTo(testArray));
    }
    // FIELDS
    // TODO data members for TestTypeConversion

    // TODO constructors and methods for TestTypeConversion
}
