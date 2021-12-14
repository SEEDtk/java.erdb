/**
 *
 */
package org.theseed.java.erdb.types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import org.junit.jupiter.api.Test;
import org.theseed.locations.Location;

/**
 * @author Bruce Parrello
 *
 */
class TestTypeConversion {

    @Test
    void testBlobConversion()  {
        double[] doubleArray = new double[] { 1e-10, 22222.22222, 333.333, 4.4e-4, 1.0, 2.0, 7e10 };
        byte[] byteArray = DbDoubleArray.doubleToBytes(doubleArray);
        double[] testArray = DbDoubleArray.bytesToDouble(byteArray);
        assertThat(testArray, equalTo(doubleArray));
        doubleArray = new double[0];
        byteArray = DbDoubleArray.doubleToBytes(doubleArray);
        testArray = DbDoubleArray.bytesToDouble(byteArray);
        assertThat(testArray, equalTo(doubleArray));
    }

    @Test
    void testLocationConversion() {
        Location loc1 = Location.create("83333.183:contig1", 100, 300);
        Location loc2 = Location.create("83333.183:contig1", 150, 50);
        Location loc3 = Location.create("83333.183:contig2", 2000, 4000);
        Location loc4 = Location.create("83333.183:contig1", 150, 50);
        String loc1String = DbLocation.locToString(loc1);
        String loc2String = DbLocation.locToString(loc2);
        String loc3String = DbLocation.locToString(loc3);
        String loc4String = DbLocation.locToString(loc4);
        assertThat(loc1String.compareTo(loc2String), greaterThan(0));
        assertThat(loc2String.compareTo(loc3String), lessThan(0));
        assertThat(loc3String.compareTo(loc1String), greaterThan(0));
        assertThat(loc2String.compareTo(loc4String), equalTo(0));
        Location test1 = DbLocation.stringToLoc(loc1String);
        Location test2 = DbLocation.stringToLoc(loc2String);
        Location test3 = DbLocation.stringToLoc(loc3String);
        assertThat(test1, equalTo(loc1));
        assertThat(test2, equalTo(loc2));
        assertThat(test3, equalTo(loc3));
    }

}
