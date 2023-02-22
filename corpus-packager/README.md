# CC-MAIN-2021-31-PDF-UNTRUNCATED #

**THIS IS A WORK IN PROGRESS!!!  Still much to be done!!!** 

## Background
[Common Crawl](https://commoncrawl.org/) gathers billions of files from the internet monthly
and makes those files available via [Amazon Public Datasets](https://registry.opendata.aws/commoncrawl/) 
under this [license and terms of use](https://commoncrawl.org/terms-of-use/full/).

The goal of this corpus is to gather the PDFs from a single crawl [CC-MAIN-2021-31](https://commoncrawl.org/2021/08/july-august-2021-crawl-archive-available/)
to support research by the PDF industry, software developers and file forensics analysts.

## Types of Common Crawl Data used
This project used two types of data from Common Crawl

### Common Crawl indices
The [indices](https://data.commoncrawl.org/crawl-data/CC-MAIN-2021-31/cc-index.paths.gz) are
gzipped text files, where each line is a JSON object that contains metadata about each URL.
Information includes, among other things: URL, mime, detected mime, the CC WARC (Web ARChive) file where the file's warc file exists along with the warc file's offset and length

### Common Crawl WARCs
Common Crawl concatenates gzipped WARCs into very large WARC files.  To fetch
an individual file's original WARC, users need to know the source WARC file, the offset for the individual file and the
length.  See below for a [worked example](#how-to-extract-an-individual-warc-from-common-crawl).

## File Types
Our team processed the [indices](https://data.commoncrawl.org/crawl-data/CC-MAIN-2021-31/cc-index.paths.gz) for this crawl 
and extracted all files where an http-header alleged that a file was a PDF or where Common Crawl's automatic file 
detection detected a PDF. We acknowledge that this choice will result in files that are not actually PDFs.

## Common Crawl or Refetched
A well known limitation of Common Crawl is that the project truncates data at 1 MB. We wanted the complete files.
In the indices for a crawl, Common Crawl has a flag for whether or not the file was truncated.  We extracted
roughly 6 million files directly from Common Crawl.  We then refetched from the original URLs nearly 2 million 
files that Common Crawl had identified as truncated.

## Filenaming
We sorted the files by sha256 and then numbered them from 0 (`0000000.pdf`) to roughly 
8 million (`7932877.pdf`).  We added a `.pdf` file extension to every file.

## Supplementary Metadata
We include several tables to link the files back to the original records in the Common Crawl and to
offer a richer view of the data via extracted metadata.

### Crawl Data
The table `cc-provenance-table.csv.gz` contains all provenance information.

* `url_id` -- project-internal id for a specific target url extracted from Common Crawl's index file.
* `file_name` -- name of the file as our project named it inside the zip
* `url` -- target url extracted from Common Crawl's index files
* `cc_digest` -- digest calculated by Common Crawl and extracted from the index files
* `cc_http_mime` -- mime as extracted from Common Crawl's index files -- this derives from the http header
* `cc_detected_mime` -- the detected mime as extracted from Common Crawl's index files.
* `cc_warc_file_name` -- the Common Crawl warc file where the file's individual warc file is stored
* `cc_warc_start` -- the offset within the `cc_warc_file` where the individual warc file is stored
* `cc_warc_end` -- this is the end of the individual warc file within the larger `cc_warc_file`
* `host_id` -- this is a project-internal foreign key for the url's host
* `cc_truncated` -- this is Common Crawl's code for why the file was truncated if the file was truncated.  This information was extracted from Common Crawl's indices. Values include:
  * `''` (6,383,873) -- (empty string) -- Common Crawls records this as not truncated
  * `length` (2,020,913) -- the file was truncated because of length
  * `disconnect` (5,861) -- there was a network disconnection during Common Crawl's fetch
  * `time` (56) -- there was a timeout during Common Crawl's fetch
* `fetched_status` -- records our project's status for obtaining the file. Values include:
  * `ADDED_TO_REPOSITORY` (6,377,619) -- extracted from Common Crawl
  * `REFETCHED_SUCCESS` (1,922,505) -- our project refetched content from the original target URL
  * `REFETCH_UNHAPPY_HOST` (53,038) -- we tried to refetch a URL, but the failures from that host exceeded our threshold.  (We didn't want to bother a host that had refused our refetches)
  * `REFETCHED_IO_EXCEPTION_READING_ENTITY` (45,561) -- during our refetch, there was an IOException while trying to read the contents
  * `EMPTY_PAYLOAD` (5,719) -- There was an empty payload in the Common Crawl warc file.
  * `REFETCHED_TIMEOUT` (5,157) -- timeout during our attempted refetch.
  * `REFETCHED_IO_EXCEPTION` (569) -- general IOException while trying to refetch.
  * `null` (506) -- ??
  * `FETCHED_EXCEPTION_EMITTING` (29) -- there was an exception trying to write to S3
* `fetched_digest` -- the sha256 that we calculated on the bytes that we have for the file
* `fetched_length` -- the length in bytes of the file that we extracted from Common Crawl or refetched

### Hosts

* `id` -- primary key to be used in joins with the `host_id` column in `cc-provenance-table.csv.gz`
* `host` -- host
* `tld` -- top level domain
* `ip_address` -- as retrieved from Common Crawl or captured during refetch
* `country`, `latitude` and `longitude` -- as geolocated by MaxMind's [geolite2](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data)

### PDFInfo
TBD

## How to extract an individual WARC from Common Crawl
First, users need the `cc_warc_file`, the `cc_warc_start` and the `cc_warc_end`.
We'll use `curl` and `gunzip`. Let's say we want to pull `0000000.pdf` which comes from `crawl-data/CC-MAIN-2021-31/segments/1627046154042.23/warc/CC-MAIN-20210731011529-20210731041529-00143.warc.gz`
starting at offset `3,724,499` and ends at offset `3,742,341` (inclusive).
1. Prepend `https://data.commoncrawl.org/` to the `cc_warc_file` to get the URL.
2. The http range will be: `3724499-3742341`
3. Fetch the gzipped WARC file: `curl -r 3724499-3742341 https://data.commoncrawl.org/crawl-data/CC-MAIN-2021-31/segments/1627046154042.23/warc/CC-MAIN-20210731011529-20210731041529-00143.warc.gz -o 0000000.warc.gz`
4. `gunzip 0000000.warc.gz`

## Credits
TBD