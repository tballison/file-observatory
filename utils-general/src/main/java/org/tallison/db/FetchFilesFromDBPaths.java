package org.tallison.db;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

public class FetchFilesFromDBPaths {

    public static void main(String[] args) throws Exception {
        Path tikaConfigFile = Paths.get("/Users/allison/Desktop/tika-config.xml");
        PipesIterator pipesIterator = PipesIterator.build(tikaConfigFile);
        Fetcher fetcher = FetcherManager.load(tikaConfigFile).getFetcher("s3f");
        Path outputRoot = Paths.get("/Users/allison/Desktop/clam-pdfs");

        for (FetchEmitTuple t : pipesIterator) {
            String clamav = t.getMetadata().get("clamav_detect");
            Matcher m = Pattern.compile("([0-9a-f]{10,})").matcher(t.getFetchKey().getFetchKey());
            String sha256 = "";
            if (m.find()) {
                sha256 = m.group(1);
            }
            Path targ = outputRoot.resolve(clamav).resolve(sha256);
            if (Files.isRegularFile(targ)) {
                continue;
            }
            Files.createDirectories(targ.getParent());
            try (InputStream is = fetcher.fetch(t.getFetchKey().getFetchKey(), new Metadata())) {
                Files.copy(is, targ, StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }
}
