# Introduction

Code for benchmarking the performance of different databases for extracting and
storing information from the [Hathitrust Extracted Features Dataset](https://wiki.htrc.illinois.edu/display/COM/Extracted+Features+Dataset).
Here we compare `postgres` and `timescaledb`.

# Data

Download the a subset of 1000 files from the corpus:

```
rsync -av --no-relative --files-from samples.txt data.analytics.hathitrust.org::features/ data/
```

# Run containers

Start the containers defined in `docker-compose.yml`:

```
docker-compose up
```

Stop the containers using `Ctrl-C`.

# Run benchmark

For help:

```
python benchmark.py -h
```

Run for each databse:
```
python benchmark -db postgres
```
