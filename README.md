# Sparkify Spark ELT S3 to  Parquet
This project extracts the million song dataset and app usage log data in json format from an s3 bucket, loads it in into a spark context
and performs transformations to allow for a star schema of fact and dimension tables. This allows for analysis of songplays/
On Artist, Song, Time and User dimensions.


## Set up

### clone repository
```bash
git clone https://github.com/Mabloq/Sparkify_Spark_S3_Parquet.git
cd setup-test
```

### Create and activate conda evn / virtual env
```bash
conda create -n myenv python=3
source activate myenv
```
### run setup.py
```bash
python setup.py install
```

### run etl
```bash
python etl.py
```

### run test analytical queries
```bash
pyhton analytical_queries.py
```

## Justification of Partitions
### Songs
I created custom partitions for the song data, the default spark partitoning scheme would not suffice as the
data is a bit skewed. For example there is quite a bit of songs that don't have year data about 20-25% of the 
the songs have 0 as the year it was released so to distribute the work better amongs the node I partioned the
songs first by year ranges(0-0, 1920-1961, 1920-1979, 1980 - 1999, 2000 - 2009, 2010 - Present) Then within
the range the songs were partioned by year, then partitioned by artist_id.

### Songplays
Song play data was more tame, so I simply partitioned by year and then month

### Time
Time is a uniform data set, so I simply partitioned it by year and then month




