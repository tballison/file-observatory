package org.tallison.cc.fetcherlite;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tika.exception.TikaException;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.pipesiterator.PipesIterator;
import org.apache.tika.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.cc.index.AbstractRecordProcessor;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * This is a lighter class that doesn't rely on a database
 * to extract files from CC and write a list of truncated urls.
 *
 * Support only for http, not s3.
 */
public class CCFileFetcherLiteCLI {

  private static Logger LOGGER = LoggerFactory.getLogger(CCFileFetcherLiteCLI.class);
  private static final String CC_DATA_PREFIX = "https://data.commoncrawl.org/cc-index/collections/CC-MAIN-2023-06/indexes/cdx-00000.gz";

  public static void main(String[] args) throws IOException {
    FetcherLiteConfig fetcherLiteConfig = new ObjectMapper().readValue(new File(args[0]), FetcherLiteConfig.class);
    execute(fetcherLiteConfig);
  }

  private static void execute(FetcherLiteConfig fetcherLiteConfig) {

  }

  private static class IndexWorker implements Callable<Integer> {

    private final ArrayBlockingQueue<String> indexUrls;
    private final AbstractRecordProcessor recordProcessor;
    private final long max;
    private final AtomicLong totalProcessed;

    IndexWorker(ArrayBlockingQueue<String> indexUrls,
        AbstractRecordProcessor recordProcessor, long max,
        AtomicLong processed) {
      this.indexUrls = indexUrls;
      this.recordProcessor = recordProcessor;
      this.max = max;
      this.totalProcessed = processed;
    }

    @Override
    public Integer call() throws Exception {
      while (true) {
        String indexUrl = indexUrls.poll(5, TimeUnit.MINUTES);
        if (indexUrl == null) {
          throw new TimeoutException("waited 5 minutes for a new record");
        }

        if (StringUtils.isBlank(indexUrl)) {
          recordProcessor.close();
          //hang forever
          indexUrls.put(StringUtils.EMPTY);
          return 1;
        }
        LOGGER.trace(indexUrl);
        processFile(indexUrl, recordProcessor);
      }
    }

    private void processFile(String url, AbstractRecordProcessor recordProcessor) {
      if (max > 0 && totalProcessed.get() >= max) {
        LOGGER.info("hit max stopping now");
        return;
      }
      LOGGER.info("processing {}", url);

      try (InputStream is =
               new BufferedInputStream(new GZIPInputStream(new URL(url).openStream()))) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
          String line = reader.readLine();
          int lines = 0;
          while (line != null) {
            LOGGER.trace("about to add a line");
            if (StringUtils.isBlank(line)) {
              line = reader.readLine();
              continue;
            }
            try {
              recordProcessor.process(line);
            } catch (IOException e) {
              LOGGER.warn("bad json: "+line);
            }
            long processed = totalProcessed.incrementAndGet();
            if (max > 0 && processed >= max) {
              LOGGER.info("hit max stopping now");
              return;
            }
            if (processed % 100000 == 0) {
              LOGGER.info("Processed " + processed);
            }
            lines++;
            line = reader.readLine();
          }
        }
      } catch (IOException e) {
        LOGGER.warn("ugh", e);
        //TODO revisit this.
        throw new RuntimeException(e);
      }
    }
  }

  private static class IndexPathsReader implements Callable<Integer> {
    //list of indexPaths files to read
    //e.g. https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-06/cc-index.paths.gz
    //https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-33/cc-index.paths.gz
    private final Path indexPathLists;
    //this is a list index paths
    //e.g. cc-index/collections/CC-MAIN-2023-06/indexes/cdx-00000.gz
    private final ArrayBlockingQueue<String> indexFiles = new ArrayBlockingQueue<>(100);

    private IndexPathsReader(Path indexPathLists) {
      this.indexPathLists = indexPathLists;
    }

    @Override
    public Integer call() throws Exception {
        try (BufferedReader reader = Files.newBufferedReader(indexPathLists)) {
          String line = reader.readLine();
          while (line != null) {
            try {
              loadIndexPaths(line, indexFiles);
            } catch (Exception e) {
              LOGGER.warn("problem loading index path: " + line, e);
            }
            line = reader.readLine();
          }
        }
      return 1;
    }

    private void loadIndexPaths(String indexPathFile,
        ArrayBlockingQueue<String> indexFiles) throws IOException, InterruptedException {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(
          new URL(indexPathFile).openStream(), StandardCharsets.UTF_8))) {
        String line = reader.readLine();
        while (line != null) {
          if (line.endsWith(".gz")) {
            //hangs permanently
            indexFiles.put(line);
          }
          line = reader.readLine();
        }
      }
    }
  }

  private static class TruncatedURLWriter implements Callable<Integer> {
    private final ArrayBlockingQueue<String> truncatedUrls;
    private final Path truncatedUrlFile;

    private TruncatedURLWriter(ArrayBlockingQueue<String> truncatedUrls,
        Path truncatedUrlFile) {
      this.truncatedUrls = truncatedUrls;
      this.truncatedUrlFile = truncatedUrlFile;
    }

    @Override
    public Integer call() throws Exception {
      try (BufferedWriter writer = Files.newBufferedWriter(truncatedUrlFile, StandardCharsets.UTF_8)) {
        while (true) {
          //blocks forever
          String url = truncatedUrls.take();
          if (url.equals(StringUtils.EMPTY)) {
            return 1;
          }
          url = url.replaceAll("[\r\n]", " ");
          writer.write(url + "\n");
        }
      }
    }
  }
}
