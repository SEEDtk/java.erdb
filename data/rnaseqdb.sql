CREATE TABLE FeatureGroup (
    group_id VARCHAR(100) PRIMARY KEY,
    group_type VARCHAR(20) NOT NULL,
    group_name VARCHAR(200) NOT NULL /* the name is usually a longer version of the ID */
    );
    CREATE INDEX idx_GroupName ON FeatureGroup (group_type, group_name);
    INSERT INTO _diagram (table_name, rloc, cloc, description) VALUES ('FeatureGroup', 1, 1,
        'A group represents a collection of features.  It is identified by an ID that is usually a short string and is often prefixed by a genome ID.');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('FeatureGroup', 'group_type', 'STRING',
        'type can be "subsystem", "modulon", "operon", and so forth');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('FeatureGroup', 'group_name', 'STRING',
        'longer version of the ID for multi-genome groups; for single-genome groups, usually the ID without the genome ID prefix');


CREATE TABLE Genome (
    genome_id VARCHAR(20) PRIMARY KEY,
    genome_name VARCHAR(200) NOT NULL
    );
    INSERT INTO _diagram (table_name, rloc, cloc, description) VALUES ('Genome', 3, 5,
        'A genome represents the reference genoem for a set of samples.  It is also characterized by its features.');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('Genome', 'genome_name', 'STRING',
        'scientific name (generally with a descriptive suffix) for the genome');


CREATE TABLE Feature (
    fig_id VARCHAR(32) PRIMARY KEY,
    gene_name VARCHAR(6),
    alias VARCHAR(32),
    genome_id VARCHAR(20) NOT NULL,
    seq_no INTEGER NOT NULL,
    assignment VARCHAR(200) NOT NULL,
    location VARCHAR(70) NOT NULL,
    FOREIGN KEY(genome_id) REFERENCES Genome(genome_id) ON DELETE CASCADE
    );
    CREATE UNIQUE INDEX idx_Genome2Feature ON Feature (genome_id, seq_no);
    CREATE INDEX idx_FeatureAlias ON Feature (alias);
    CREATE INDEX idx_FeatureGene ON Feature (gene_name);
    INSERT INTO _diagram (table_name, rloc, cloc, description) VALUES ('Feature', 1, 5,
        'A feature belongs to one genome, and has an optional common name (e.g. "dnaK") and an alias.  At some point we may need to allow multiple aliases.  The feature ID is the standard FIG ID from PATRIC.');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('Feature', 'seq_no', 'INTEGER',
        'sequence number of the feature (0-based) indicating its position in the "feat_data" array of the RnaSample records');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('Feature', 'location', 'LOCATION',
        'the chromosome location of the feature in the genome');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('Feature', 'assignment', 'STRING',
        'the functional assignment of the feature, as determined by RAST');



CREATE TABLE FeatureToGroup (
    fig_id VARCHAR(32) NOT NULL,
    group_id VARCHAR(32) NOT NULL,
    FOREIGN KEY(fig_id) REFERENCES Feature(fig_id) ON DELETE CASCADE,
    FOREIGN KEY(group_id) REFERENCES FeatureGroup(group_id) ON DELETE CASCADE
    );
    CREATE UNIQUE INDEX idx_Group2Feature ON FeatureToGroup (fig_id, group_id);
    CREATE UNIQUE INDEX idx_Feature2Group ON FeatureToGroup (group_id, fig_id);
    INSERT INTO _diagram (table_name, rloc, cloc, description) VALUES ('FeatureToGroup', 1, 3,
        'This table contains the many-to-many relationship between features and groups.  Groups are, of course, designed to contain multiple features, but it is very common for a feature to be in multiple groups, and even multiple groups of the same type.');

CREATE TABLE SampleCluster (
    cluster_id VARCHAR(30) NOT NULL PRIMARY KEY,
    height INTEGER NOT NULL,
    score DOUBLE NOT NULL,
    numSamples INTEGER NOT NULL
    );
    INSERT INTO _diagram (table_name, rloc, cloc, description) VALUES ('SampleCluster', 3, 1,
        'A sample cluster represents a group of samples for a single genome that are in a similar expression state.  The clustering is determined based on mean scaled difference between expression levels on a feature-by-feature basis.  The ID consists of a genome ID, a colon, and a cluster ID.');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('SampleCluster', 'height', 'INTEGER',
        'height of the binary tree of samples produced to create the cluster');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('SampleCluster', 'score', 'DOUBLE',
        'clustering score, which is currently the minimum similarity between samples in the cluster');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('SampleCluster', 'numSamples', 'INTEGER',
        'number of samples in the cluster');

CREATE TABLE RnaSample (
    sample_id VARCHAR(50) NOT NULL PRIMARY KEY,
    genome_id VARCHAR(20) NOT NULL,
    process_date DOUBLE,
    read_count INTEGER NOT NULL,
    base_count INTEGER NOT NULL,
    quality DOUBLE NOT NULL,
    feat_count INTEGER NOT NULL,
    feat_data BLOB NOT NULL,
    suspicious TINYINT NOT NULL,
    cluster_id VARCHAR(30),
    pubmed INTEGER,
    project_id VARCHAR(30),
    FOREIGN KEY(genome_id) REFERENCES Genome(genome_id) ON DELETE CASCADE,
    FOREIGN KEY(cluster_id) REFERENCES SampleCluster(cluster_id) ON DELETE CASCADE
    );
    CREATE INDEX idx_Genome2Sample ON RnaSample (genome_id, process_date DESC);
    CREATE INDEX idx_Cluster2Sample ON RnaSample (cluster_id, quality DESC);
    CREATE INDEX idx_PubmedSample ON RnaSample (pubmed, genome_id);
    CREATE INDEX idx_ProjectSample ON RnaSample (project_id, genome_id);
    INSERT INTO _diagram (table_name, rloc, cloc, description) VALUES ('RnaSample', 3, 3,
        'This table contains the data for a single sample.  A sample always belongs to one genome.  The expression data is stored in an array of doubles (BLOB), with missing values stored as NaN.  The Feature table contains the indices for finding individual features in the array.  This limits our query ability, but greatly improves performance.');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('RnaSample', 'process_date', 'DATE',
        'processing date of the sample; this is the date it was processed by our pipeline, not the date it was collected');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('RnaSample', 'quality', 'DOUBLE',
        'percentage of reads with good mapping to features (MAPQ >= 30) according to SAMSTAT');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('RnaSample', 'feat_count', 'INTEGER',
        'number of features with expression values');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('RnaSample', 'read_count', 'INTEGER',
        'number of reads in the sample, an indicator of coverage');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('RnaSample', 'base_count', 'INTEGER',
        'number of base pairs in the sample (generally proportional to read_count, but the proportion varies)');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('RnaSample', 'pubmed', 'INTEGER',
        'pubmed ID number for the paper relating to this sample (if any)');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('RnaSample', 'project_id', 'STRING',
        'NCBI project ID associated with this sample (if any)');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('RnaSample', 'suspicious', 'BOOLEAN',
        'TRUE if this sample is considered suspicious and should not be used in analysis');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('RnaSample', 'feat_data', 'DOUBLE_ARRAY',
        'array of expression values for this sample; missing values are coded as NaN');

CREATE TABLE Measurement (
    sample_id VARCHAR(50) NOT NULL,
    genome_id VARCHAR(20) NOT NULL,
    measure_type VARCHAR(30) NOT NULL,
    value DOUBLE NOT NULL,
    FOREIGN KEY(sample_id) REFERENCES RnaSample(sample_id) ON DELETE CASCADE,
    FOREIGN KEY(genome_id) REFERENCES Genome(genome_id) ON DELETE CASCADE
    );
    CREATE UNIQUE INDEX idx_Sample2Measure ON Measurement (sample_id, measure_type);
    CREATE INDEX idx_Genome2Measure ON Measurement(genome_id, measure_type, sample_id);
    CREATE INDEX idx_Type2Measure ON Measurement(measure_type, genome_id, value DESC);
    INSERT INTO _diagram (table_name, rloc, cloc, description) VALUES ('Measurement', 5, 3,
        'A measurement represents a numeric observation about a sample.  This could be optical density, chemical output, or even a qualitative measurement coded as 1 or 0.');
    INSERT INTO _fields (table_name, field_name, field_type, description) VALUES ('Measurement', 'measure_type', 'STRING',
        'type code for the measurement; this should be from a controlled vocabulary, but the control is not enforced');
