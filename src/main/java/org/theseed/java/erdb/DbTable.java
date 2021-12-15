/**
 *
 */
package org.theseed.java.erdb;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This object describes the metadata for a database table.
 *
 * @author Bruce Parrello
 *
 */
public class DbTable {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(DbTable.class);
    /** parent database */
    private DbConnection db;
    /** name of the table */
    private String name;
    /** map of field names to types */
    private Map<String, Field> fields;
    /** map of target table names to join equality strings, with all keys normalized to lower-case */
    private Map<String, Link> links;
    /** name of primary key */
    private String keyName;

    /**
     * This object encapsulates the diagramming info for the table.
     */
    public class Placement {

        /** row placement */
        private int row;
        /** column placement */
        private int col;
        /** table description */
        private String comment;

        /**
         * Construct this object from a query against the diagram table.
         *
         * @param result	result set positioned on this table
         *
         * @throws SQLException
         */
        private Placement(ResultSet result) throws SQLException {
            this.row = result.getInt("rloc");
            this.col = result.getInt("cloc");
            this.comment = result.getString("description");
        }

        /**
         * @return the row position
         */
        public int getRow() {
            return this.row;
        }

        /**
         * @return the column position
         */
        public int getCol() {
            return this.col;
        }

        /**
         * @return the table description
         */
        public String getComment() {
            return this.comment;
        }

    }

    /**
     * This object describes a link between two tables.  It contains the field name in this table and
     * the field name in the target table.
     */
    public static class Link {

        /** field name in this table */
        private String localField;
        /** field name in the target table */
        private String otherField;

        /**
         * Specify a link from this table to another.
         *
         * @param local		field name for this table
         * @param other		equivalent field in the other table
         */
        protected Link(String local, String other) {
            this.localField = local;
            this.otherField = other;
        }

        /**
         * Store this link in an SQL statement buffer.
         *
         * @param buffer		target SQL statement buffer
         * @param source		name of the source table
         * @param target		name of the target table
         */
        public void store(SqlBuffer buffer, String source, String target) {
            buffer.quote(source, this.localField).append(" = ").quote(target, this.otherField);
        }
    }

    /**
     * This object represents custom field data.
     */
    protected static class FieldDesc {

        /** type of field */
        protected DbType type;
        /** comment about the field for the diagram */
        protected String comment;

        /**
         * Construct a custom field data descriptor.
         *
         * @param fieldType		type of field
         * @param desc			descriptive comment about the field
         */
        public FieldDesc(DbType fieldType, String desc) {
            this.type = fieldType;
            if (desc == null)
                this.comment = "";
            else
                this.comment = desc;
        }

    }

    /**
     * This object describes a field in a table.
     */
    public class Field {

        /** name of the field */
        private String name;
        /** data type of the field */
        private DbType type;
        /** TRUE if the field is nullable */
        private boolean nullable;
        /** comment relating to this field */
        private String comment;

        /**
         * Construct a specific field descriptor.
         *
         * @param name		name of the field
         * @param type		data type of the field
         * @param nullable	TRUE if the field is nullable
         */
        public Field(String name, DbType type, boolean nullable) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
            this.comment = "";
        }

        /**
         * Construct a field descriptor from a metadata result.
         *
         * @param db			parent database
         * @param result		result set positioned on the field
         * @param customTypes	map of field names to custom types
         */
        protected Field(ResultSet result, Map<String, FieldDesc> customTypes) {
            try {
                DbConnection db = DbTable.this.db;
                this.name = result.getString("COLUMN_NAME");
                // Check for a custom type.  If none is found, compute the real type.
                FieldDesc desc = customTypes.get(this.name);
                if (desc == null) {
                    this.type = DbType.parse(db, result.getString("TYPE_NAME"));
                    this.comment = "";
                } else {
                    this.type = desc.type;
                    this.comment = desc.comment;
                }
                // Get the nullability flag.
                this.nullable = (result.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
            } catch (SQLException e) {
                throw new RuntimeException("Error reading metadata: " + e.getMessage());
            }
        }

        /**
         * @return the name of the field
         */
        public String getName() {
            return this.name;
        }

        /**
         * @return the type of the field
         */
        public DbType getType() {
            return this.type;
        }

        /**
         * @return TRUE if the field is nullable
         */
        public boolean isNullable() {
            return this.nullable;
        }

        /**
         * Store a reference to this field in an SQL buffer.
         *
         * @param buffer	SQL buffer to receive the field
         */
        public void store(SqlBuffer buffer) {
            buffer.quote(DbTable.this.name, this.name);
        }

        /**
         * @return the field's descriptive comment
         */
        public String getComment() {
            return comment;
        }

    }

    /**
     * Create the metadata for a table.
     *
     * @param db		parent database connection
     * @param name		name of a table
     *
     * @return the table metadata, or NULL if the table does not exist
     *
     * @throws SQLException
     */
    protected static DbTable load(DbConnection db, String name) throws SQLException {
        // Get the catalog and the schema.
        String catalog = db.getCatalog();
        String schema = db.getSchema();
        // Get the metadata.
        DatabaseMetaData meta = db.getMetaData();
        // Read the custom-type fields from the meta-table.
        Map<String, FieldDesc> customTypes = db.getCustomTypes(name);
        // Read the table columns.
        ResultSet results = meta.getColumns(catalog, schema, name, null);
        // Create the DbTable object.  We may need to throw it away later.
        DbTable retVal = new DbTable();
        retVal.db = db;
        retVal.name = name;
        // Build the field map.
        var fields = new TreeMap<String, Field>();
        while (results.next()) {
            Field fDesc = retVal.new Field(results, customTypes);
            fields.put(fDesc.name, fDesc);
        }
        // Fail if there are no fields.
        if (fields.isEmpty()) {
            retVal = null;
        } else {
            // Save the field map.
            retVal.fields = fields;
            // Get the primary key.  If there is more than one field we just pick the first.
            results = meta.getPrimaryKeys(null, null, name);
            if (! results.next())
                retVal.keyName = null;
            else {
                retVal.keyName = results.getString("COLUMN_NAME");
                if (results.next()) {
                    // Here we have no primary key as the ERDB model defines it.  That's ok, so long as this
                    // table is not the target of a 1-to-many relationship.
                    retVal.keyName = null;
                }
            }
            // Get the foreign keys (if any).  We link each connected table name to its join condition.
            retVal.links = new TreeMap<String, Link>();
            // The imported keys are many-to-one relationships:  the target table's primary key appears in retVal.table.
            results = meta.getImportedKeys(catalog, schema, name);
            while (results.next())
                retVal.storeLink(results);
            // The exported keys are many-to-one relationships:  retVal.table's primary key appears in the target table.
            results = meta.getExportedKeys(catalog, schema, name);
            while (results.next())
                retVal.storeLink(results);
            if (log.isInfoEnabled()) {
                String pkey = (retVal.keyName == null ? "(none)" : retVal.keyName);
                log.info("{} columns and {} foreign keys found in table {} with primary key {}.",
                    retVal.fields.size(), retVal.links.size(), name, pkey);
            }
        }
        return retVal;
    }

    /**
     * Store the metadata for a link to another table.
     *
     * @param results			result set positioned on the current link
     *
     * @throws SQLException
     */
    private void storeLink(ResultSet results) throws SQLException {
        if (results.getInt("KEY_SEQ") != 1)
            throw new SQLException("NOT SUPPORTED: Multi-field link found in table " + name + ".");
        // Form the join condition.
        String link1Name = results.getString("PKTABLE_NAME").toLowerCase();
        String link1Col = results.getString("PKCOLUMN_NAME");
        String link2Name = results.getString("FKTABLE_NAME").toLowerCase();
        String link2Col = results.getString("FKCOLUMN_NAME");
        // Determine which table is not us.  That is the target table.
        if (link1Name.contentEquals(this.name.toLowerCase())) {
            // Here we are linking out.
            this.links.put(link2Name, new Link(link1Col, link2Col));
        } else {
            // Here we are linking in.
            this.links.put(link1Name, new Link(link2Col, link1Col));
        }
    }

    /**
     * @return the name of the table
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return the type of a field
     *
     * @param fName		name of the field whose type is desired
     *
     * @throws SQLException
     */
    public DbType getType(String fName) throws SQLException {
        DbTable.Field field = this.getField(fName);
        return field.type;
    }

    /**
     * @return TRUE if a field is nullable, else FALSE
     *
     * @param fName		name of the field whose nullability needs to be known
     *
     * @throws SQLException
     */
    public boolean isNullable(String fName) throws SQLException {
        DbTable.Field field = this.getField(fName);
        return field.nullable;
    }

    /**
     * @return a list of the field descriptors for this table
     */
    public Collection<Field> getFields() {
        return this.fields.values();
    }

    /**
     * @return the join condition for getting to the other table, or NULL if there is none
     *
     * @param tName		name of other table to which to link
     */
    public Link getLink(String tName) {
        return this.links.get(tName.toLowerCase());
    }

    /**
     * @return the name of the primary key (or NULL if no single field is the primary key)
     */
    public String getKeyName() {
        return this.keyName;
    }

    /**
     * @return the descriptor for the named field
     *
     * @param field		name of the desired field
     *
     * @throws SQLException
     */
    public Field getField(String field) throws SQLException {
        DbTable.Field retVal = this.fields.get(field);
        if (field == null)
            throw new SQLException("No field \"" + field + "\" found in " + this.name);
        return retVal;
    }

    /**
     * @return the placement data for this table, or NULL if there is none
     *
     * @throws SQLException
     */
    public Placement getPlacement() throws SQLException {
        ResultSet result = this.db.loadPlacementRecord(this.name);
        Placement retVal = null;
        if (result.next())
            retVal = this.new Placement(result);
        result.close();
        return retVal;
    }


}
