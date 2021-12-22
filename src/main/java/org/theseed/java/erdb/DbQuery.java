/**
 *
 */
package org.theseed.java.erdb;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.java.erdb.DbTable.Field;
import org.theseed.java.erdb.types.DbBoolean;
import org.theseed.java.erdb.types.DbDate;
import org.theseed.java.erdb.types.DbDouble;
import org.theseed.java.erdb.types.DbInteger;
import org.theseed.java.erdb.types.DbLocation;
import org.theseed.java.erdb.types.DbString;
import org.theseed.locations.Location;

/**
 * This object holds a prepared statement for a database query.  The constructor specifies the path
 * through the database.  The user then adds fields and conditions using a fluent interface.
 * To execute the statement, the parameter marks must be filled in with actual values.  Each
 * record returned by the query is a map of field names to values.
 *
 * Field names are specified using a dot notation-- "NNN.YYY", where "NNN" is the table name and "YYY"
 * is the field name.
 *
 * The path through the database is specified using delimited table names.  A delimiter of "<" indicates
 * a left join, a space indicates a normal join, and an "&" indicates a join to a previous table.
 * Extra spaces around the delimiters are ignored. If a table is used more than once, the additional uses
 * should be name unique by added a number at the end.  Table names themselves cannot end in digits,
 * so all digits at the end are considered part of the distinguishing number.  The name/number combination
 * is used as an alias for the table.
 *
 * When using an ampersand (&), the table after the ampersand is not joined with the previous.  Rather, the
 * next table is reset as the starting point of the join after that.  This enables very complex query
 * paths.
 *
 * This query can be used multiple times with different parameters.  The parameters are assigned an index
 * (starting from 1) in the order that they are created.  Use the "set" methods to update the parameters.
 * Each time an iterator is created, a new result set is fired off.
 *
 * @author Bruce Parrello
 *
 */
public class DbQuery implements AutoCloseable, Iterable<DbRecord> {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(DbQuery.class);
    /** prepared statement for the query */
    private PreparedStatement stmt;
    /** map of table name aliases to table descriptors */
    private Map<String, DbTable> tableMap;
    /** FROM clause */
    private SqlBuffer fromClause;
    /** WHERE clause */
    private SqlBuffer whereClause;
    /** ORDER BY clause */
    private SqlBuffer orderByClause;
    /** list of field types for the returned fields */
    private List<DbType> fieldTypes;
    /** list of field specs for the returned fields */
    private List<String> fieldNames;
    /** set of field specs already in the select clause */
    private Set<String> fieldsUsed;
    /** target database */
    private DbConnection db;
    /** list of saved parameter values */
    private List<DbValue> parms;
    /** map of delimiters to join types */
    private static final Map<String, String> JOIN_TYPES = Map.of(
            "", " INNER JOIN",
            "<", " LEFT OUTER JOIN");
    /** pattern for finding the first table in the table path */
    private static final Pattern FIRST_TABLE = Pattern.compile("[^\\s<>]+");
    /** pattern for parsing the joined tables in the table path */
    private static final Pattern JOINED_TABLE = Pattern.compile("(\\s+|\\s*[<&]\\s*)([^\\s<&]+)");

    /**
     * Construct a database query.
     *
     * @param qdb			database to query
     * @param tablePath		path through the database tables
     */
    public DbQuery(DbConnection qdb, String tablePath) throws SQLException {
        this.init(qdb);
        // Now we must parse the path.
        this.parsePath(tablePath);
    }

    /**
     * Initialize the query for the specified database.
     *
     * @param qdb	database to query
     */
    private void init(DbConnection qdb) {
        this.db = qdb;
        // Denote that so far we have no statement prepared.
        this.stmt = null;
        // Set up the list for the parameter value holders.
        this.parms = new ArrayList<>();
        // Create the buffers for the various statement clauses.
        this.orderByClause = new SqlBuffer(db);
        this.whereClause = new SqlBuffer(db).startList(" AND ");
        this.fromClause = new SqlBuffer(db);
        // We maintain two parallel lists for the returned fields.  These enable us to construct the
        // select clause and the field map / holder list for the prepared query statement.
        this.fieldTypes = new ArrayList<>();
        this.fieldNames = new ArrayList<>();
        // Finally, we use a set object to prevent duplicate fields.
        this.fieldsUsed = new TreeSet<>();
        // Create the table map.  This enables us to get the table descriptors for all the table names
        // and aliases.
        this.tableMap = new TreeMap<>();
    }

    /**
     * Parse the path to create the join clauses and get the table definitions.  At the end of
     * this process, all the table names and aliases will be in the table map and we will have
     * a fully-formed FROM clause.
     *
     * @param tablePath		path through the tables (as described above)
     *
     * @throws SQLException
     */
    private void parsePath(String tablePath) throws SQLException {
        Matcher m = FIRST_TABLE.matcher(tablePath);
        // Get the first table.
        if (! m.find())
            throw new SQLException("No tables found in path \"" + tablePath + "\".");
        String oldTable = m.group();
        DbTable oldDescriptor = this.findTable(oldTable);
        this.addTable(oldTable, oldDescriptor);
        // Switch to tables preceded by delimiters.
        m.usePattern(JOINED_TABLE);
        // Loop through the rest of the path.
        while (m.find()) {
            // Here we have a delimiter in group 1 and a table spec in group 2.
            String delim = StringUtils.trim(m.group(1));
            String newTable = m.group(2);
            DbTable newDescriptor = this.findTable(newTable);
            // Check for the special ampersand case.
            if (! delim.contentEquals("&")) {
                // Store the join connector in the FROM clause and put in the target table.
                this.fromClause.append(JOIN_TYPES.get(delim));
                this.addTable(newTable, newDescriptor);
                // Get the WHERE sub-clause from the descriptor.  Note we use "getName" to get the
                // real table name.
                DbTable.Link joinLink = oldDescriptor.getLink(newDescriptor.getName());
                if (joinLink == null)
                    throw new SQLException("No path from " + oldTable + " to " + newTable + ".");
                joinLink.store(this.fromClause.append(" ON "), oldTable, newTable);
            }
            // Set up for the next join.
            oldTable = newTable;
            oldDescriptor = newDescriptor;
        }
    }

    /**
     * Locate a table's descriptor and store it in the table map under the specified alias.
     *
     * @param tableSpec		table specification (table name with optional number suffix)
     *
     * @return the table descriptor for a table specification
     */
    protected DbTable findTable(String tableSpec) throws SQLException {
        // Check for a number suffix.
        int nameEnd = tableSpec.length() - 1;
        while (nameEnd >= 0 && Character.isDigit(tableSpec.charAt(nameEnd)))
            nameEnd--;
        // Form the real name.
        String realName = tableSpec.substring(0, nameEnd + 1);
        if (realName.isEmpty())
            throw new SQLException("Invalid numeric table name specified in table path.");
        // Get the table descriptor.
        DbTable retVal = this.db.getTable(realName);
        if (retVal == null)
            throw new SQLException("Cannot find table \"" + realName + "\" in this database.");
        // Add the name and descriptor to the table map.
        this.tableMap.put(tableSpec, retVal);
        return retVal;
    }

    /**
     * Add a table to the WHERE clause.  The table is stored in the WHERE clause
     * and assigned the specified alias.
     *
     * @param tableSpec			alias name of this table
     * @param descriptor		table descriptor containing the real name
     */
    private void addTable(String tableSpec, DbTable descriptor) {
        String realName = descriptor.getName();
        this.fromClause.append(" ").quote(realName);
        if (! realName.contentEquals(tableSpec))
            this.fromClause.append(" AS ").quote(tableSpec);
    }

    /**
     * This creates the iterator.  Only one iterator can exist.  Multiple requests return the same object
     * and the next() method is simply synchronized.
     *
     * Note that iterators can't throw checked exceptions, so we must rethrow SQLException errors as
     * runtime exceptions.
     *
     * @return the iterator for this query's records
     */
    public Iterator<DbRecord> iterator() {
        try {
            return this.new Iter();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This is the iterator through the results of the query.  Creating it starts the query.
     *
     * @author Bruce Parrello
     */
    public class Iter implements Iterator<DbRecord> {

        /** result set for the query */
        private ResultSet results;
        /** next record to return */
        private DbRecord nextRecord;

        private Iter() throws SQLException {
            // Insure the query is started.
            this.results = DbQuery.this.startQuery();
            // Denote we have no next record.
            this.nextRecord = null;
        }

        @Override
        public boolean hasNext() {
            if (this.nextRecord == null && this.results != null)
                this.getNextRecord();
            return (this.nextRecord != null);
        }

        @Override
        public DbRecord next() {
            DbRecord retVal = null;
            if (this.nextRecord == null && this.results != null)
                this.getNextRecord();
            if (this.nextRecord == null)
                throw new NoSuchElementException("Attempt to read past end of query.");
            else {
                retVal = this.nextRecord;
                this.nextRecord = null;
            }
            return retVal;
        }

        /**
         * Advance to the next record in the result set.
         */
        private void getNextRecord() {
            try {
                if (this.results.next()) {
                    // Here we have another record to return.
                    this.nextRecord = new DbRecord(this.results, DbQuery.this.fieldNames,
                            DbQuery.this.fieldTypes);
                } else {
                    this.results.close();
                    this.results = null;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Close the current result set.
         *
         * @throws SQLException
         */
        public void close() throws SQLException {
            if (this.results != null) {
                this.results.close();
                this.results = null;
            }
        }

    }

    /**
     * The spliterator is unordered, and asking for it executes the query.
     *
     * @return a spliterator for this query
     */
    public Spliterator<DbRecord> spliterator() {
        return Spliterators.spliteratorUnknownSize(this.iterator(), Spliterator.IMMUTABLE + Spliterator.NONNULL);
    }

    /**
     * Execute the query with the current parameters.
     *
     * @return the result set for the query
     *
     * @throws SQLException
     */
    private synchronized ResultSet startQuery() throws SQLException {
        if (this.stmt == null) {
            // Assemble the SQL.
            SqlBuffer stmtBuffer = new SqlBuffer(this.db);
            if (this.fieldNames.isEmpty())
                throw new SQLException("Query does not have fields to select.");
            // Form the select clause.
            stmtBuffer.append("SELECT ").quoteSpec(this.fieldNames.get(0));
            final int n = this.fieldNames.size();
            this.fieldNames.subList(1, n).stream().forEach(x -> stmtBuffer.append(", ").quoteSpec(x));
            // Add the FROM clause.
            stmtBuffer.append(" FROM ").append(this.fromClause);
            // Add the optional filter and ordering clauses.
            if (! this.whereClause.isEmpty())
                stmtBuffer.append(" WHERE ").append(this.whereClause);
            if (! this.orderByClause.isEmpty())
                stmtBuffer.append(" ORDER BY ").append(this.orderByClause);
            // Create the statement.
            this.stmt = this.db.createStatement(stmtBuffer);
        }
        // Update the parameters.
        final int n = this.parms.size();
        for (int i = 0; i < n; i++)
            this.parms.get(i).store(this.stmt, i+1);
        // Get the result set.
        ResultSet retVal = this.stmt.executeQuery();
        return retVal;
    }

    /**
     * Here the records come back as a stream. The stream is unordered, and asking
     * for it executes the query.
     *
     * @return a stream of the records in this query
     */
    public Stream<DbRecord> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }

    @Override
    public void close() throws SQLException {
        // If we have a statement, close it.
        if (this.stmt != null)
            this.stmt.close();
    }

    /**
     * Add a set of table fields to the SELECT clause.  Fields that are already present will be
     * ignored.
     *
     * @param table		table specification for the fields
     * @param fields	array of the fields to add
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbQuery select(String table, String... fields) throws SQLException {
        // Get the table descriptor.
        DbTable descriptor = this.getSpecTable(table);
        // Loop through the fields.
        for (String field : fields) {
            DbTable.Field fieldDescriptor = descriptor.getField(field);
            this.selectField(table, fieldDescriptor);
        }
        return this;
    }

    /**
     * @return the table descriptor for a table spec
     *
     * @param table		table specification for the table
     *
     * @throws SQLException
     */
    private DbTable getSpecTable(String table) throws SQLException {
        DbTable retVal = this.tableMap.get(table);
        if (retVal == null)
            throw new SQLException("Table \"" + table + "\" is not present in this query.");
        return retVal;
    }

    /**
     * Add all fields for the specified table to the SELECT clause.  Fields that are already present
     * will be ignored.
     *
     * @param table		table specification for the fields
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbQuery selectAll(String table) throws SQLException {
        // Get the table descriptor.
        DbTable descriptor = this.getSpecTable(table);
        // Loop through the list of fields.
        for (DbTable.Field field : descriptor.getFields()) {
            this.selectField(table, field);
        }
        return this;
    }

    /**
     * Add the specified field in the specified table to the SELECT list.  A holder is created
     * for the field and the field spec is placed in the select clause
     *
     * @param table		target table spec
     * @param field		field name
     *
     * @throws SQLException
     */
    private void selectField(String table, Field field) throws SQLException {
        // Create the field reference name.
        String refName = table + "." + field.getName();
        // Verify that this field is new.
        if (! this.fieldsUsed.contains(refName)) {
            // It is, so add it.
            this.fieldsUsed.add(refName);
            this.fieldNames.add(refName);
            this.fieldTypes.add(field.getType());
        }
    }

    /**
     * Add an ordering criterion to the query.
     *
     * @param field		field spec for the ordering (table.field)
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbQuery orderBy(String field) throws SQLException {
        this.findComparableField(field);
        this.orderByClause.appendDelim().quoteSpec(field);
        return this;
    }

    /**
     * This locates the descriptor for a field based on the field spec, and fails if
     * the field is not found or it is not comparable.
     *
     * @param field		field specification (table.field)
     *
     * @return the field descriptor
     *
     * @throws SQLException
     */
    private DbTable.Field findComparableField(String field) throws SQLException {
        DbTable.Field retVal = this.findField(field);
        if (! retVal.getType().isComparable())
            throw new SQLException("Field \"" + field + "\" cannot be used for sorting or filtering.");
        return retVal;
    }

    /**
     * @return the descriptor for a field
     *
     * @param field		field specification (table.field)
     *
     * @throws SQLException
     */
    private DbTable.Field findField(String field) throws SQLException {
        String[] parts = StringUtils.split(field, ".");
        DbTable table = this.findTable(parts[0]);
        DbTable.Field retVal = table.getField(parts[1]);
        if (retVal == null)
            throw new SQLException("Field \"" + parts[1] + "\" not found in table " + table.getName() + ".");
        return retVal;
    }

    /**
     * Add one or more parameter holders for the specified field.
     *
     * @param field		field spec (table.field) relevant to the markers
     * @param count		number of markers to add
     *
     * @throws SQLException
     */
    private void addHolders(String field, int count) throws SQLException {
        DbTable.Field fieldDescriptor = this.findComparableField(field);
        // Create the parameter value holders.
        for (int i = 0; i < count; i++) {
            DbValue holder = fieldDescriptor.getType().create();
            this.parms.add(holder);
        }
    }

    /**
     * Add a relational filter to the query.  The filter will be of the form
     *
     * 		field op ?
     *
     * @param field		field spec (table.field) for field being filtered
     * @param op		relational operator
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbQuery rel(String field, Relop op) throws SQLException {
        // Create the holders.
        this.addHolders(field, 1);
        // Update the WHERE clause.
        this.whereClause.appendDelim().quoteSpec(field).append(op.text()).appendMark();
        return this;
    }

    /**
     * Add a BETWEEN filter to the query.  The filter will be of the form
     *
     * 		field BETWEEN ? AND ?
     *
     * @param field		field spec (table.field) for field being filtered
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbQuery between(String field) throws SQLException {
        // Create the holders.
        this.addHolders(field, 2);
        // Update the WHERE clause.
        this.whereClause.appendDelim().quoteSpec(field).append(" BETWEEN ").appendMark()
                .append(" AND ").appendMark();
        return this;
    }

    /**
     * Add an IN filter to the query.  The filter will be of the form
     *
     * 		field IN (?,?,?,?)
     *
     * with the appropriate number of parameter marks.
     *
     * @param field		field spec (table.field) for field being filtered
     * @param count		number of parameter marks to use
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbQuery in(String field, int count) throws SQLException {
        // Create the holders.
        this.addHolders(field, count);
        // Update the WHERE clause.
        this.whereClause.appendDelim().quoteSpec(field).append(" IN ").addMarkList(count);
        return this;
    }

    /**
     * Add an IS NULL or IS NOT NULL filter to the query.  This filter does not
     * add parameter marks.
     *
     * @param field		field spec (table.field) for the field being filtered
     * @param flag		TRUE for is-null, FALSE for is-not-null
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbQuery isNull(String field, boolean flag) throws SQLException {
        DbTable.Field fieldDescriptor = this.findComparableField(field);
        if (! fieldDescriptor.isNullable())
            throw new SQLException("Field " + field + " is not nullable.");
        this.whereClause.appendDelim().quoteSpec(field).append(" IS");
        if (! flag)
            this.whereClause.append(" NOT");
        this.whereClause.append(" NULL");
        return this;

    }

    /**
     * Insure there is room for the specified number of parameter values.
     *
     * @param idx		index (1-based) of first parameter
     * @param length	number of parameter values to store
     * @throws SQLException
     */
    private void validateParmSet(int idx, int length) throws SQLException {
        int needed = idx - 1 + length;
        if (needed > this.parms.size())
            throw new SQLException("Attempt to store " + length + " parameters at position " + idx
                    + " but only " + this.parms.size() + " slots available.");
    }

    /**
     * Get the holder for the specified parameter and verify its type.
     *
     * @param idx		index (1-based) of the parameter
     * @param class1	required parameter type
     *
     * @return the parameter's holder object
     *
     * @throws SQLException
     */
    private DbValue getParm(int idx, Class<? extends DbValue> class1) throws SQLException {
        DbValue retVal = this.parms.get(idx - 1);
        if (! class1.isAssignableFrom(retVal.getClass()))
            throw new SQLException("Parameter " + idx + " is not of type " + class1.toString());
        return retVal;
    }

    /**
     * Store integers in the parameter list.
     *
     * @param idx		index (1-based) of the first parameter to store
     * @param values	integer values to store
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbQuery setParm(int idx, int... values) throws SQLException {
        this.validateParmSet(idx, values.length);
        // Position on the indicated first parameter holder.
        int i = idx;
        // Fill in the slots.
        for (int value : values) {
            DbInteger intHolder = (DbInteger) this.getParm(i, DbInteger.class);
            intHolder.set(value);
            i++;
        }
        return this;
    }

    /**
     * Store boolean values in the parameter list.
     *
     * @param idx		index (1-based) of the first parameter to store
     * @param values	boolean values to store
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbQuery setParm(int idx, boolean... values) throws SQLException {
        this.validateParmSet(idx, values.length);
        // Position on the indicated first parameter holder.
        int i = idx;
        // Fill in the slots.
        for (boolean value : values) {
            DbBoolean boolHolder = (DbBoolean) this.getParm(i, DbBoolean.class);
            boolHolder.set(value);
            i++;
        }
        return this;
    }

    /**
     * Store floating-point values in the parameter list.
     *
     * @param idx		index (1-based) of the first parameter to store
     * @param values	floating-point values to store
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbQuery setParm(int idx, double... values) throws SQLException {
        this.validateParmSet(idx, values.length);
        // Position on the indicated first parameter holder.
        int i = idx;
        // Fill in the slots.
        for (double value : values) {
            DbDouble dblHolder = (DbDouble) this.getParm(i, DbDouble.class);
            dblHolder.set(value);
            i++;
        }
        return this;
    }

    /**
     * Store string values in the parameter list.
     *
     * @param idx		index (1-based) of the first parameter to store
     * @param values	string values to store
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbQuery setParm(int idx, String... values) throws SQLException {
        this.validateParmSet(idx, values.length);
        // Position on the indicated first parameter holder.
        int i = idx;
        // Fill in the slots.
        for (String value : values) {
            DbString strHolder = (DbString) this.getParm(i, DbString.class);
            strHolder.set(value);
            i++;
        }
        return this;
    }

    /**
     * Store location values in the parameter list.
     *
     * @param idx		index (1-based) of the first parameter to store
     * @param values	location values to store
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbQuery setParm(int idx, Location... values) throws SQLException {
        this.validateParmSet(idx, values.length);
        // Position on the indicated first parameter holder.
        int i = idx;
        // Fill in the slots.
        for (Location value : values) {
            DbLocation locHolder = (DbLocation) this.getParm(i, DbLocation.class);
            locHolder.set(value);
            i++;
        }
        return this;
    }

    /**
     * Store date/time values in the parameter list.
     *
     * @param idx		index (1-based) of the first parameter to store
     * @param values	date/time values to store
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbQuery setParm(int idx, Instant... values) throws SQLException {
        this.validateParmSet(idx, values.length);
        // Position on the indicated first parameter holder.
        int i = idx;
        // Fill in the slots.
        for (Instant value : values) {
            DbDate dateHolder = (DbDate) this.getParm(i, DbDate.class);
            dateHolder.set(value);
            i++;
        }
        return this;
    }

    /**
     * Store local date values in the parameter list.
     *
     * @param idx		index (1-based) of the first parameter to store
     * @param values	local date values to store
     *
     * @return this object, for fluent invocation
     *
     * @throws SQLException
     */
    public DbQuery setParm(int idx, LocalDate... values) throws SQLException {
        this.validateParmSet(idx, values.length);
        // Position on the indicated first parameter holder.
        int i = idx;
        // Fill in the slots.
        for (LocalDate value : values) {
            DbDate dateHolder = (DbDate) this.getParm(i, DbDate.class);
            dateHolder.set(value);
            i++;
        }
        return this;
    }

}
