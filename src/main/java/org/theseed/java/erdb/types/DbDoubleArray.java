/**
 *
 */
package org.theseed.java.erdb.types;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;

import org.theseed.java.erdb.DbValue;

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

    /**
     * Convert an array of doubles into a byte array.
     *
     * @param val	array of doubles to convert
     *
     * @return a byte array containing the content of the double array
     */
    protected static byte[] doubleToBytes(double[] val) {
        // Create a buffer for the array.
        ByteBuffer bytes = ByteBuffer.allocate(Double.BYTES * val.length);
        // Store the values in the buffer.
        Arrays.stream(val).forEach(x -> bytes.putDouble(x));
        // Return the result as a byte array.
        return bytes.array();
    }

    /**
     * Convert a byte array into an array of doubles.
     *
     * @param val	byte array to convert
     *
     * @return an array of doubles represented by the byte array
     */
    protected static double[] bytesToDouble(byte[] val) {
        // Get the bytes as an array of doubles.
        DoubleBuffer doubles = ByteBuffer.wrap(val).asDoubleBuffer();
        // Copy them into a destination array.
        double[] retVal = new double[doubles.remaining()];
        doubles.get(retVal);
        // Return the result.
        return retVal;
    }

    @Override
    protected void storeValue(PreparedStatement stmt, int idx) throws SQLException {
        // Convert the array to a byte buffer.
        byte[] buffer = doubleToBytes(this.value);
        // Store it as a blob.
        stmt.setBytes(idx, buffer);
    }

    @Override
    protected void fetchValue(ResultSet results, int idx) throws SQLException {
        // Get the blob from the database.
        byte[] blob = results.getBytes(idx);
        this.value = bytesToDouble(blob);
    }

    @Override
    public int getInt() throws SQLException {
        throw new SQLException("Cannot represent a DOUBLE_ARRAY value as an integer.");
    }

    @Override
    public double getDouble() throws SQLException {
        throw new SQLException("Cannot represent a DOUBLE_ARRAY value as a floating-point.");
    }

    @Override
    public String getString() throws SQLException {
        throw new SQLException("Cannot represent a DOUBLE_ARRAY value as a string.");
    }

    @Override
    protected int getSqlType() {
        return Types.BLOB;
    }

    /**
     * Store an array in this value holder.
     *
     * @param array		array to store
     */
    public void set(double[] array) {
        this.value = array;
        this.setNotNull();
    }

}
