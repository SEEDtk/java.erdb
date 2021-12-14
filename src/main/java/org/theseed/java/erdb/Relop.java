/**
 *
 */
package org.theseed.java.erdb;

/**
 * This is an enumeration for relational operators.
 *
 * @author Bruce Parrello
 *
 */
public enum Relop {
    EQ {
        @Override
        public String text() {
            return " = ";
        }
    }, GE {
        @Override
        public String text() {
            return " >= ";
        }
    }, GT {
        @Override
        public String text() {
            return " > ";
        }
    }, LE {
        @Override
        public String text() {
            return " <= ";
        }
    }, LT {
        @Override
        public String text() {
            return " < ";
        }
    }, NE {
        @Override
        public String text() {
            return " != ";
        }
    };

    /**
     * @return the string representation of the relational operator
     */
    public abstract String text();


}
