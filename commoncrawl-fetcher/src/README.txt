This is a set of utilities for extracting files from Common Crawl.

The assumption is that you don't have direct access to S3 and you
need to pull data.

Step 1:
  * Download the 300 index .gz files (this is normally ~1 TB)

Step 2:
  * Read through the .gz files and index into postgres those files that
    meet certain criteria (maybe just PDFs, etc)

Step 3:
  * Based on the records in the database, request the warc file from AWS for
    each file
  * Extract the literal bytes from that file and index some more data from the warc

Step 4:
  * For each file that CC identified as truncated, go back to the original URL and try
    to retrieve the file from there.