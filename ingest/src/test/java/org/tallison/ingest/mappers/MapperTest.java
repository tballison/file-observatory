package org.tallison.ingest.mappers;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import org.junit.Test;

public class MapperTest {

    InputStream getPath(String relPath) throws IOException {
        try {
            String path = "/test-documents/"+relPath;
            return Files.newInputStream(Paths.get(this.getClass().getResource(path).toURI()));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Test
    public void testDateParsing() throws Exception {
        String v = "Mon Apr  1 22:12:30 2013 UTC";
        v = v.replaceAll("\\s+", " ").trim();
        Instant instant = LocalDateTime.parse(v,
                        DateTimeFormatter.ofPattern( "EEE MMM d HH:mm:ss yyyy z",
                                Locale.US )
                )
                .atZone(ZoneId.of("UTC")).toInstant();
        System.out.println(instant);

    }
}
