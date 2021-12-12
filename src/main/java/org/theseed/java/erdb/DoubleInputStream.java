/**
 *
 */
package org.theseed.java.erdb;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This object converts a double array to an input stream, which in turn allows a double
 * array to be turned into a blob.
 *
 * @author Bruce Parrello
 *
 */
public class DoubleInputStream extends InputStream {

    // FIELDS
    /** original floating-point array */
    private double[] array;
    /** index of next buffer element */
    private int arrayIdx;
    /** byte buffer for the current buffer element */
    private ByteBuffer current;
    /** position of next byte in byte buffer */
    private int currIdx;

    /**
     * Construct the stream from a floating-point array.
     *
     * @param source	floating-point array to convert
     */
    public DoubleInputStream(double[] source) {
        this.array = source;
        // Denote the next array element is the first.
        this.arrayIdx = 0;
        // Create the buffer and denote that we need to refill it.
        this.current = ByteBuffer.allocate(Double.BYTES);
        this.currIdx = Double.BYTES;
    }

    @Override
    public int read() throws IOException {
        if (this.currIdx >= Double.BYTES) {
            // Here we need to refill the byte buffer.
            if (arrayIdx >= this.array.length) {
                // Here we are at end-of-stream.
                return -1;
            } else {
                // Get the next array element.
                this.current.putDouble(0, arrayIdx);
                this.arrayIdx++;
                this.currIdx = 0;
            }
        }
        // If we are here, there is data in the buffer and currIdx is in range.
        int retVal = this.current.get(this.currIdx);
        // Push past this byte to the next one.
        this.currIdx++;
        return retVal;
    }

}
