# CC-MAIN-2021-31-PDF-UNTRUNCATED #

**THIS IS A WORK IN PROGRESS!!!  Still much to be done!!!** 

## Background
[Common Crawl](https://commoncrawl.org/) gathers billions of files from the internet monthly
and makes those files available via [Amazon Public Datasets](https://registry.opendata.aws/commoncrawl/) 
under this [license and terms of use](https://commoncrawl.org/terms-of-use/full/).

The goal of this corpus is to gather the PDFs from a single crawl [CC-MAIN-2021-31](https://commoncrawl.org/2021/08/july-august-2021-crawl-archive-available/)
to support research by the PDF industry, software developers and file forensics analysts.

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
8 million (`8000000.pdf`).  We added a `.pdf` file extension to every file, no matter whether
it was detected as a PDF or not and no matter what extension the file had in the URL.

## Supplementary Metadata
We include several tables to link the files back to the original records in the Common Crawl and to
offer a richer view of the data via extracted metadata.

### Crawl Data

### PDFInfo

## Credits
TBD