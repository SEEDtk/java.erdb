/**
 *
 */
package org.theseed.java.erdb.types;

import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.theseed.java.erdb.DbValue;
import org.theseed.java.erdb.DoubleInputStream;

/**
 * This object holds a basic floating-point array.  The default is an empty array.
 *
 * The floating-point array is stored as a blob in the database.
 *
 * @author Bruce Parrello
 *
 */
public class DbDoubleArray extends DbValue {

    // FIELDS
    /** value of this object */
    private double[] value;
    /** default value */
    private static final double[] DEFAULT = new double[0];

    /**
     * Create a floating-point array with the default value.
     */
    public DbDoubleArray() {
        this.value = DEFAULT;
    }

    /**
     * Create a floating-point array with the specified value.
     */
    public DbDoubleArray(double[] val) {
        this.value = val;
    }

    /**
     * @return the value of this object.
     */
    public double[] get() {
        return this.value;
    }

    @Override
    protected void store(PreparedStatement stmt, int idx) throws SQLException {
        // Get an input stream for the array.
        DoubleInputStream dataStream = new DoubleInputStream(this.value);
        // Store it as a blob.
        stmt.setBlob(idx, dataStream, Double.BYTES * this.value.length);
    }

    @Override
    protected void fetch(ResultSet results, int idx) throws SQLException {
        // Get the blob from the database.
        Blob blob = results.getBlob(idx);
        this.value = blobToArray(blob);
    }

    /**
     * Convert a blob into an array of doubles.
     *
     * @param blob		blob to convert
     *
     * @return an array of doubles built from the blob
     *
     * @throws SQLException
     */
    protected static double[] blobToArray(Blob blob) throws SQLException {
        // Allocate a double array of the appropriate length.  Note we toss extra bytes instead of throwing
        // an error.
        double[] retVal = new double[(int) (blob.length() / Double.BYTES)];
        // Get a byte buffer for conversion.
        ByteBuffer buffer = ByteBuffer.wrap(blob.getBytes(0, retVal.length * Double.BYTES));
        // Fill the array from the byte buffer.
        for (int i = 0; i < retVal.length; i++)
            retVal[i] = buffer.getDouble();
        return retVal;
    }

}
