CREATE TABLE FeatureGroup (
	/* A group represents a collection of features.  It is identified by an ID that is usually a short string. */
	group_id VARCHAR(100) PRIMARY KEY,
	group_type VARCHAR(20) NOT NULL, /* type can be "subsystem", "modulon", "operon", and so forth */
	group_name VARCHAR(200) NOT NULL /* the name is usually a longer version of the ID */
	);
	CREATE UNIQUE INDEX idx_GroupName ON FeatureGroup (group_type, group_name);

CREATE TABLE Genome (
	/* A genome represents the reference genome for a set of samples.  It is also characterized by its features. */
	genome_id VARCHAR(20) PRIMARY KEY,
	genome_name VARCHAR(200) NOT NULL /* this is the scientific name of the genome */
	);

CREATE TABLE Feature (
	/* A feature belongs to one genome and has an optional common name and alias. */
	fig_id VARCHAR(32) PRIMARY KEY,
	gene_name VARCHAR(6), /* this is the common gene name */
	alias VARCHAR(32), /* this is an alias; we only keep one for our purposes */
	genome_id VARCHAR(20) NOT NULL, /* the link to the owning genome */
	seq_no INTEGER NOT NULL, /* the sequence number of the feature, which tells us its position in the expression array */
	assignment VARCHAR(200) NOT NULL, /* the functional assignment of the feature */
	location VARCHAR(70) NOT NULL, /* the chromosome location of the feature */
	FOREIGN KEY(genome_id) REFERENCES Genome(genome_id) ON DELETE CASCADE
	);
	CREATE UNIQUE INDEX idx_Genome2Feature ON Feature (genome_id, seq_no);
	CREATE INDEX idx_FeatureAlias ON Feature (alias);
	CREATE INDEX idx_FeatureGene ON Feature (gene_name);
	INSERT INTO _fields (table_name, field_name, field_type) VALUES ('Feature', 'location', 'LOCATION');


CREATE TABLE FeatureToGroup (
	/* This table creates the many-to-many relationship between features and groups.  Most groups exist in a single
	   genome, but some (like subsystems) are in multiple genomes. */
	fig_id VARCHAR(32) NOT NULL,
	group_id VARCHAR(32) NOT NULL,
	FOREIGN KEY(fig_id) REFERENCES Feature(fig_id) ON DELETE CASCADE,
	FOREIGN KEY(group_id) REFERENCES FeatureGroup(group_id) ON DELETE CASCADE
	);
	CREATE UNIQUE INDEX idx_Group2Feature ON FeatureToGroup (fig_id, group_id);
	CREATE UNIQUE INDEX idx_Feature2Group ON FeatureToGroup (group_id, fig_id);

CREATE TABLE RnaSample (
	/* This table contains the data for a single sample.  A sample always relates to a single genome.  The sample data
	   is stored as a blob that unpacks into an array of doubles.  A missing value is stored as NaN.  The Feature table
	   contains the array index into this array for each feature.  This limits our query ability, but saves on the amount
	   of reading we have to do. */
	sample_id VARCHAR(50) NOT NULL PRIMARY KEY,
	genome_id VARCHAR(20) NOT NULL, /* genome used to map this sample */
	process_date DOUBLE, /* processing date of the sample, in Julian days */
	read_count INTEGER NOT NULL, /* number of reads */
	base_count INTEGER NOT NULL, /* sample size in base pairs */
	quality DOUBLE NOT NULL, /* quality rating (% >= MAP30) according to SAMSTAT */
	feat_count INTEGER NOT NULL, /* number of features with expression values */
	feat_data BLOB NOT NULL, /* array of expression data values (TPM) */
	suspicious BOOLEAN NOT NULL, /* TRUE (1) if this is a suspicious sample, else FALSE */
	cluster_id VARCHAR(30), /* ID of the sample cluster to which this sample belongs */
	pubmed INTEGER, /* pubmed ID of the paper associated with this sample (if any) */
	project_id VARCHAR(30), /* NCBI project associated with this sample (if any) */
	FOREIGN KEY(genome_id) REFERENCES Genome(genome_id) ON DELETE CASCADE,
	FOREIGN KEY(cluster_id) REFERENCES SampleCluster(cluster_id) ON DELETE CASCADE
	);
	CREATE INDEX idx_Genome2Sample ON RnaSample (genome_id, process_date DESC);
	CREATE INDEX idx_Cluster2Sample ON RnaSample (cluster_id, quality DESC);
	CREATE INDEX idx_PubmedSample ON RnaSample (pubmed, genome_id) WHERE pubmed IS NOT NULL;
	CREATE INDEX idx_ProjectSample ON RnaSample (project_id, genome_id) WHERE project_id IS NOT NULL;
	INSERT INTO _fields (table_name, field_name, field_type) VALUES ('RnaSample', 'process_date', 'DATE');
	INSERT INTO _fields (table_name, field_name, field_type) VALUES ('RnaSample', 'suspicious', 'BOOLEAN');
	INSERT INTO _fields (table_name, field_name, field_type) VALUES ('RnaSample', 'feat_data', 'DOUBLE_ARRAY');

CREATE TABLE SampleCluster (
	/* A sample cluster represents a group of samples for a single genome that are in a similar
	   expression state.  The clustering is determined based on mean scaled difference between
	   expression levels. The cluster ID is prefixed by the genome ID (e.g. 511145.183.CL1) */
	cluster_id VARCHAR(30) NOT NULL PRIMARY KEY,
	height INTEGER NOT NULL, /* height of the cluster as determined during formation */
	score DOUBLE NOT NULL, /* similarity score associated with the cluster */
	numSamples INTEGER NOT NULL /* number of samples in the cluster */
	);
CREATE TABLE Measurement (
	/* A measurement represents a numeric observation about a sample.  This could be optical density, chemical output or
	   utilization level, or presence or absence of a characteristic (e.g. 1 = healthy, 0 = diseased). */
	id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	sample_id VARCHAR(50) NOT NULL, /* sample being measured */
	genome_id VARCHAR(20) NOT NULL, /* genome to which the sample belongs */
	measure_type VARCHAR(30) NOT NULL, /* type code for the quantity being measured */
	value DOUBLE NOT NULL, /* value being measured */
	FOREIGN KEY(sample_id) REFERENCES RnaSample(sample_id) ON DELETE CASCADE,
	FOREIGN KEY(genome_id) REFERENCES Genome(genome_id) ON DELETE CASCADE
	);
	CREATE UNIQUE INDEX idx_Sample2Measure ON Measurement (sample_id, measure_type);
	CREATE INDEX idx_Genome2Measure ON Measurement(genome_id, measure_type, sample_id);
	CREATE INDEX idx_Type2Measure ON Measurement(measure_type, genome_id, value DESC);
