package org.tallison.cc.fetcherlite;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.TikaEmitterException;
import org.apache.tika.pipes.fetcher.FetchKey;
import org.apache.tika.utils.StringUtils;
import org.netpreserve.jwarc.*;
import org.tallison.cc.CCFileFetcher;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

public class FileFromCCWarcFetcher {
  private void processTuple(String url) throws IOException,
      TikaException, SQLException {

    byte[] warcRecordGZBytes = fetch(url);
    try {
      parseWarc(url, warcRecordGZBytes);
    } catch (IOException e) {
      LOGGER.warn("problem parsing warc file", e);
      writeStatus(t.getId(), CCFileFetcher.FETCH_STATUS.FETCHED_IO_EXCEPTION_READING_ENTITY, insert);
      return;
    }
  }

  private void processRecord(FetchEmitTuple t, WarcRecord record) throws SQLException,
      IOException {

    if (!((record instanceof WarcResponse) &&
        record.contentType().base().equals(MediaType.HTTP))) {
      return;
    }
    String truncated = t.getMetadata().get("cc_truncated");
    String warcDigest = t.getMetadata().get("cc_index_digest");
    String ipAddress = "";

    Optional<InetAddress> inetAddress = ((WarcResponse) record).ipAddress();

    if (inetAddress.isPresent()) {
      ipAddress = inetAddress.get().getHostAddress();
    }

    Optional<String> httpContentLengthString =
        ((WarcResponse) record).http().headers().first(
            "Content-Length");

    long httpContentLength = -1;
    if (httpContentLengthString.isPresent()) {
      try {
        httpContentLength = Long.parseLong(httpContentLengthString.get());
      } catch (NumberFormatException e) {

      }
    }
    if (!StringUtils.isBlank(truncated)) {
      writeStatus(t.getId(), CCFileFetcher.FETCH_STATUS.TRUNCATED, httpContentLength, ipAddress,
          insert);
    } else {
      fetchPayload(t, httpContentLength, ipAddress, warcDigest, record);
    }
  }

  private void fetchPayload(FetchEmitTuple t, long httpContentLength,
      String ipAddress, String warcDigest,
      WarcRecord record)
      throws IOException, SQLException {
    Optional<WarcPayload> payload = ((WarcResponse) record).payload();
    if (!payload.isPresent()) {
      LOGGER.warn("no payload {}", t.getId());
      return;
    }
    if (payload.get().body().size() == 0) {
      LOGGER.warn("empty payload id={}", t.getId());
      writeStatus(t.getId(), CCFileFetcher.FETCH_STATUS.EMPTY_PAYLOAD,
          httpContentLength,
          ipAddress, insert);
      return;
    }

    Path tmp = Files.createTempFile("ccfile-fetcher-", "");
    try {

      Files.copy(payload.get().body().stream(), tmp, StandardCopyOption.REPLACE_EXISTING);
      String targetDigest = null;
      long tmpLength = 0l;
      String sha1digest = "";
      try (InputStream is = Files.newInputStream(tmp)) {
        sha1digest = base32.encodeAsString(DigestUtils.sha1(is));
      } catch (IOException e) {
        writeStatus(t.getId(), CCFileFetcher.FETCH_STATUS.FETCHED_IO_EXCEPTION_DIGESTING,
            httpContentLength,
            ipAddress, insert);
        LOGGER.warn("IOException during digesting: " + tmp.toAbsolutePath());
        return;
      }

      try (InputStream is = Files.newInputStream(tmp)) {
        targetDigest = DigestUtils.sha256Hex(is);
        tmpLength = Files.size(tmp);
      } catch (IOException e) {
        writeStatus(t.getId(), CCFileFetcher.FETCH_STATUS.FETCHED_IO_EXCEPTION_DIGESTING,
            httpContentLength,
            ipAddress, insert);
        LOGGER.warn("IOException during digesting: " + tmp.toAbsolutePath());
        return;
      }

      if (! sha1digest.equals(warcDigest)) {
        LOGGER.warn("Conflicting digests id={}", t.getId());
      }
      String targetPath =
          targetDigest.substring(0, 2) + "/" + targetDigest.substring(2, 4) + "/" +
              targetDigest;
      Metadata metadata = new Metadata();

      try (InputStream is = TikaInputStream.get(tmp, metadata)) {
        emitter.emit(targetPath, is, metadata);
      } catch (AmazonS3Exception e) {
        if (e.getMessage() != null && e.getMessage().contains("Please reduce your " +
            "request rate")) {
          LOGGER.warn("throttling -- aws exception -- please reduce your request " +
                  "rate",
              t.getId(), e);
          try {
            Thread.sleep(500);
          } catch (InterruptedException ex) {
            //swallow
          }
        } else {
          LOGGER.warn("problem emitting id={}", t.getId(), e);
        }
        writeStatus(t.getId(), CCFileFetcher.FETCH_STATUS.FETCHED_EXCEPTION_EMITTING, targetDigest,
            tmpLength, httpContentLength, ipAddress, insert);
        emitException++;
        return;
      } catch (TikaEmitterException | IOException e) {
        writeStatus(t.getId(), CCFileFetcher.FETCH_STATUS.FETCHED_EXCEPTION_EMITTING, targetDigest,
            tmpLength, httpContentLength, ipAddress, insert);
        LOGGER.warn("problem emitting id={}", t.getId(), e);
        emitException++;
        return;
      }
      if (sha1digest.equals(warcDigest)) {
        writeStatus(t.getId(), CCFileFetcher.FETCH_STATUS.ADDED_TO_REPOSITORY, targetDigest, tmpLength,
            httpContentLength, ipAddress, insert);
      } else {
        writeStatus(t.getId(), CCFileFetcher.FETCH_STATUS.ADDED_TO_REPOSITORY_DIFF_DIGEST,
            targetDigest, tmpLength,
            httpContentLength, ipAddress, insert);
      }
    } finally {
      deleteTmp(tmp);
    }
  }

  private void parseWarc(FetchEmitTuple t, byte[] warcRecordGZBytes) throws SQLException,
      IOException {
    //need to leave initial inputstream open while parsing warcrecord
    //can't just parse record and return
    try (InputStream is = new GZIPInputStream(
        new ByteArrayInputStream(warcRecordGZBytes))) {
      try (WarcReader warcreader = new WarcReader(is)) {

        //should be a single warc per file
        //return the first
        for (WarcRecord record : warcreader) {
          processRecord(t, record);
          return;
        }
      }
    }
  }

  private byte[] fetch(FetchEmitTuple t) throws TikaException, IOException {

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    FetchKey fetchKey = t.getFetchKey();
    try (InputStream is = fetcher.fetch(fetchKey.getFetchKey(), fetchKey.getRangeStart(),
        fetchKey.getRangeEnd(), new Metadata())) {
      IOUtils.copy(is, bos);
    }
    return bos.toByteArray();
  }

}
