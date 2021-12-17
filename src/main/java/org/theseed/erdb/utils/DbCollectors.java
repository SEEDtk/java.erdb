/**
 *
 */
package org.theseed.erdb.utils;

/**
 * This is a static class that provides easy access to the DbConnection collection methods.
 *
 * @author Bruce Parrello
 *
 */
public class DbCollectors {

    /**
     * Construct a database stream collector that outputs a map.
     *
     * @param key	field spec (table.field) for the field containing the string key
     */
    public static MapCollector map(String key) {
        MapCollector retVal = new MapCollector();
        retVal.setKeySpec(key);
        return retVal;
    }

    /**
     * Construct a database stream collector that output a set.
     *
     * @param key	field spec (table.field) for the field containing the string key
     */
    public static SetCollector set(String key) {
        SetCollector retVal = new SetCollector();
        retVal.setKeySpec(key);
        return retVal;
    }

}
