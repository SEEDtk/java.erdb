/**
 *
 */
package org.theseed.java.erdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.erdb.utils.DbCollectors;
import org.theseed.java.erdb.types.DbDate;
import org.theseed.java.erdb.types.DbLocation;
import org.theseed.locations.Location;

/**
 * This class contains methods that can be called to stress-test databases.  It assumes the
 * connection is already made, allowing the same tests to be run for multiple DB engines.
 *
 * @author Bruce Parrello
 *
 */
public class CommonTesters {

    private static final String GENOME_NAME = "Escherichia coli K-12 MG1655 test genome";

    private static final double[] ARRAY_SAMPLE5 = new double[] { -4.6, 1.0, 2.0, 3.0, 4.0, 6.0 };

    private static final double[] ARRAY_SAMPLE3 = new double[] { 0.0, -4.7, 2.0, 3.0, 4.0, 5.0 };

    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(CommonTesters.class);

    // static fields used
    private static final Location locPeg1 = Location.create("511145.1832:NODE_1_length_927033_cov_53.679905", 2000, 3000);
    private static final Location locPeg2 = Location.create("511145.1832:NODE_1_length_927033_cov_53.679905", 2500, 3600);

    /**
     * Test loading and querying a database.
     *
     * @param db	target database
     *
     * @throws SQLException
     * @throws IOException
     */
    public static void testLoading(DbConnection db) throws SQLException, IOException {
        DbTable rnaSampleTable = db.getTable("RnaSample");
        assertThat(rnaSampleTable, nullValue());
        db.scriptUpdate(new File("data", "rnaseqdb.sql"));
        try (DbConnection.Transaction xact = db.new Transaction()) {
            try (DbLoader loader = DbLoader.batch(db, "Genome")) {
                loader.set("genome_id", "511145.183");
                loader.set("genome_name", GENOME_NAME);
                loader.insert();
            }
            try (DbLoader loader = DbLoader.batch(db, "Feature")) {
                loader.set("fig_id", "fig|511145.183.peg.1");
                loader.set("genome_id", "511145.183");
                loader.setNull("alias");
                loader.setNull("gene_name");
                loader.set("seq_no", 1);
                loader.set("assignment", "hypothetical protein");
                loader.set("location", locPeg1);
                loader.insert();
                loader.set("fig_id", "fig|511145.183.peg.2");
                loader.set("genome_id", "511145.183");
                loader.set("gene_name", "thrA");
                loader.set("seq_no", 2);
                loader.set("assignment", "concrete protein with a long function");
                loader.set("location", locPeg2);
                loader.insert();
            }
            try (DbLoader loader = DbLoader.batch(db, "SampleCluster")) {
                loader.set("cluster_id", "CL1");
                loader.set("height", 6);
                loader.set("score", 90.1);
                loader.set("numSamples", 6);
                loader.insert();
                loader.set("cluster_id", "CL2");
                loader.set("height", 5);
                loader.set("score", 80.2);
                loader.set("numSamples", 4);
                loader.insert();
            }
            try (DbLoader loader = DbLoader.batch(db, "RnaSample")) {
                loader.set("sample_id", "sample1");
                loader.set("genome_id", "511145.183");
                loader.set("process_date", LocalDate.of(2000, 10, 20));
                loader.set("read_count", 1000);
                loader.set("base_count", 2000);
                loader.set("quality", 30.0);
                loader.set("feat_data", new double[] { 1.0, 2.0, 3.0, 4.0, 5.0 });
                loader.set("feat_count", 4000);
                loader.set("suspicious", false);
                loader.setNull("cluster_id");
                loader.setNull("pubmed");
                loader.set("project_id", "project1");
                loader.insert();
                loader.set("sample_id", "sample2");
                loader.set("genome_id", "511145.183");
                loader.set("process_date", LocalDate.of(2001, 11, 21));
                loader.set("read_count", 1001);
                loader.set("base_count", 2001);
                loader.set("quality", 30.1);
                loader.set("feat_data", new double[] { 7.0, -4.6, 1.0, 2.0, 3.0, 4.0, 5.0 });
                loader.set("feat_count", 4001);
                loader.set("suspicious", true);
                loader.set("cluster_id", "CL1");
                loader.set("pubmed", 6000);
                loader.set("project_id", "project1");
                loader.insert();
                loader.set("sample_id", "sample3");
                loader.set("genome_id", "511145.183");
                loader.set("process_date", LocalDate.of(2002, 12, 22));
                loader.set("read_count", 1002);
                loader.set("base_count", 2002);
                loader.set("quality", 30.2);
                loader.set("feat_data", ARRAY_SAMPLE3);
                loader.set("feat_count", 4002);
                loader.set("suspicious", false);
                loader.set("cluster_id", "CL2");
                loader.setNull("pubmed");
                loader.setNull("project_id");
                loader.insert();
                loader.set("sample_id", "sample4");
                loader.set("genome_id", "511145.183");
                loader.set("process_date", LocalDate.of(2003, 3, 23));
                loader.set("read_count", 1004);
                loader.set("base_count", 2004);
                loader.set("quality", 30.4);
                loader.set("feat_data", ARRAY_SAMPLE5);
                loader.set("feat_count", 4004);
                loader.set("suspicious", false);
                loader.set("cluster_id", "CL2");
                loader.setNull("pubmed");
                loader.set("project_id", "project2");
                loader.insert();
                loader.set("sample_id", "sample5");
                loader.set("genome_id", "511145.183");
                loader.set("process_date", LocalDate.of(2001, 3, 23));
                loader.set("read_count", 1005);
                loader.set("base_count", 2005);
                loader.set("quality", 30.5);
                loader.set("feat_data", ARRAY_SAMPLE5);
                loader.set("feat_count", 4005);
                loader.set("suspicious", false);
                loader.set("cluster_id", "CL1");
                loader.setNull("pubmed");
                loader.set("project_id", "project2");
                loader.insert();
            }
            xact.commit();
        }
        // Now we have set up a genome, two features, and 4 samples.  Verify by reading them back.
        Set<String> allSamples = db.getKeys("RnaSample");
        assertThat(allSamples, containsInAnyOrder("sample1", "sample2", "sample3", "sample4", "sample5"));
        try (DbQuery query = new DbQuery(db, "Genome1 Feature2")) {
            query.rel("Genome1.genome_id", Relop.EQ).orderBy("Feature2.seq_no");
            query.select("Genome1", "genome_id", "genome_name");
            query.selectAll("Feature2");
            query.setParm(1, "511145.183");
            Iterator<DbRecord> iter = query.iterator();
            assertThat("No records found.", iter.hasNext());
            DbRecord record = iter.next();
            assertThat(record.getString("Genome1.genome_id"), equalTo("511145.183"));
            assertThat(record.getString("Genome1.genome_name"), equalTo(GENOME_NAME));
            assertThat(record.getString("Feature2.fig_id"), equalTo("fig|511145.183.peg.1"));
            assertThat(record.getString("Feature2.genome_id"), equalTo("511145.183"));
            assertThat(record.getInt("Feature2.seq_no"), equalTo(1));
            assertThat(record.getString("Feature2.alias"), nullValue());
            assertThat(record.getString("Feature2.gene_name"), nullValue());
            assertThat(record.getString("Feature2.assignment"), equalTo("hypothetical protein"));
            Location loc = ((DbLocation) record.getValue("Feature2.location")).get();
            assertThat(loc, equalTo(locPeg1));
            assertThat("Only one record found.", iter.hasNext());
            record = iter.next();
            assertThat(record.getString("Genome1.genome_id"), equalTo("511145.183"));
            assertThat(record.getString("Genome1.genome_name"), equalTo(GENOME_NAME));
            assertThat(record.getString("Feature2.fig_id"), equalTo("fig|511145.183.peg.2"));
            assertThat(record.getString("Feature2.genome_id"), equalTo("511145.183"));
            assertThat(record.getInt("Feature2.seq_no"), equalTo(2));
            assertThat(record.getString("Feature2.alias"), nullValue());
            assertThat(record.getString("Feature2.gene_name"), equalTo("thrA"));
            assertThat(record.getString("Feature2.assignment"), equalTo("concrete protein with a long function"));
            loc = ((DbLocation) record.getValue("Feature2.location")).get();
            assertThat(loc, equalTo(locPeg2));
            assertThat("Too many records found.", ! iter.hasNext());
        }
        try (DbQuery query = new DbQuery(db, "RnaSample SampleCluster&RnaSample Genome")) {
            query.between("RnaSample.process_date");
            query.isNull("RnaSample.pubmed", true);
            query.select("Genome", "genome_name");
            query.select("RnaSample", "sample_id", "read_count", "project_id", "pubmed", "process_date", "feat_data");
            query.select("SampleCluster", "height", "score");
            query.setParm(1, LocalDate.of(2000, 12, 10), LocalDate.of(2003, 1, 1));
            Map<String, DbRecord> results =
                    query.stream().parallel().collect(DbCollectors.map("RnaSample.sample_id"));
            assertThat(results.size(), equalTo(2));
            DbRecord sample3 = results.get("sample3");
            assertThat(sample3.getString("RnaSample.sample_id"), equalTo("sample3"));
            assertThat(sample3.getInt("RnaSample.read_count"), equalTo(1002));
            assertThat("Project ID should be null.", sample3.isNull("RnaSample.project_id"));
            assertThat("pubmed should be null.", sample3.isNull("RnaSample.pubmed"));
            assertThat("pubmed-null boolean should be false.", ! sample3.getBool("RnaSample.pubmed"));
            assertThat(sample3.getDoubleArray("RnaSample.feat_data"), equalTo(ARRAY_SAMPLE3));
            assertThat(sample3.getString("Genome.genome_name"), equalTo(GENOME_NAME));
            assertThat(sample3.getInt("SampleCluster.height"), equalTo(5));
            assertThat(sample3.getDouble("SampleCluster.score"), equalTo(80.2));
            query.setParm(1, DbDate.instantOf(2001, 1, 1), DbDate.instantOf(2004, 1, 1));
            results = query.stream().parallel().collect(DbCollectors.map("RnaSample.sample_id"));
            assertThat(results.size(), equalTo(3));
            DbRecord sample5 = results.get("sample5");
            assertThat(sample5.getString("RnaSample.sample_id"), equalTo("sample5"));
            assertThat(sample5.getDoubleArray("RnaSample.feat_data"), equalTo(ARRAY_SAMPLE5));
            assertThat(sample5.getDouble("SampleCluster.score"), equalTo(90.1));
            assertThat(results.get("sample1"), nullValue());
            assertThat(results.get("sample2"), nullValue());
            assertThat(results.get("sample3").getString("RnaSample.sample_id"), equalTo("sample3"));
            assertThat(results.get("sample4").getString("RnaSample.sample_id"), equalTo("sample4"));
        }
        try (DbQuery query = new DbQuery(db, "RnaSample SampleCluster")) {
            query.in("RnaSample.sample_id", 4);
            query.select("RnaSample", "sample_id");
            query.selectAll("SampleCluster");
            query.setParm(1, "sample1", "sample2", "sample3", "sample5");
            Map<String, DbRecord> results = query.stream().collect(DbCollectors.map("RnaSample.sample_id"));
            assertThat(results.get("sample4"), nullValue());
            assertThat(results.get("sample1"), nullValue());
            assertThat(results.get("sample2").getString("SampleCluster.cluster_id"), equalTo("CL1"));
            assertThat(results.get("sample3").getString("SampleCluster.cluster_id"), equalTo("CL2"));
            assertThat(results.get("sample5").getString("SampleCluster.cluster_id"), equalTo("CL1"));
        }
        // Now try some existence checks.
        assertThat("sample1 not found", db.checkForRecord("RnaSample", "sample1"));
        assertThat("sample6 found", ! db.checkForRecord("RnaSample", "sample6"));
        // Delete some samples.
        List<String> samples = Arrays.asList("sample1", "sample3", "sample5");
        db.deleteRecords("RnaSample", samples);
        assertThat("wrong sample deleted", db.checkForRecord("RnaSample", "sample2"));
        assertThat("delete failed", ! db.checkForRecord("RnaSample", "sample1"));
        assertThat("delete failed", ! db.checkForRecord("RnaSample", "sample3"));
        assertThat("delete failed", ! db.checkForRecord("RnaSample", "sample5"));
        // Delete the genome.
        db.deleteRecord("Genome", "511145.183");
        // Verify the genome is gone, along with all the features and samples.
        assertThat("genome not deleted", ! db.checkForRecord("Genome", "511145.183"));
        assertThat("sample2 not deleted", ! db.checkForRecord("RnaSample", "sample2"));
        assertThat("peg 2 not deleted", ! db.checkForRecord("Feature", "fig|511145.183.peg.2"));

    }

}
