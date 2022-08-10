package org.tallison.cc.pipes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

import org.apache.tika.config.Field;
import org.apache.tika.config.Initializable;
import org.apache.tika.config.InitializableProblemHandler;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.EmitKey;
import org.apache.tika.pipes.fetcher.FetchKey;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

public class CCIndexPipesIterator extends PipesIterator implements Initializable {

    Path indexPathsFile = null;
    List<String> indexPathsUrls = null;

    @Override
    protected void enqueue() throws IOException, TimeoutException, InterruptedException {
        if (indexPathsUrls != null) {
            for (String indexPathUrl : indexPathsUrls) {
                try (BufferedReader reader = getUrlReader(indexPathUrl)) {
                    String line = reader.readLine();
                    while (line != null) {
                        tryToAdd(line);
                        line = reader.readLine();
                    }
                }
            }
        } else {

        }
    }

    private BufferedReader getUrlReader(String indexPathsUrl) throws IOException {
        return new BufferedReader(
                new InputStreamReader(new GZIPInputStream(new URL(indexPathsUrl).openStream()),
                        StandardCharsets.UTF_8));
    }

    private BufferedReader getFileReader() throws IOException {
        if (indexPathsFile.endsWith(".gz")) {
            return new BufferedReader(
                    new InputStreamReader(new GZIPInputStream(Files.newInputStream(indexPathsFile)),
                            StandardCharsets.UTF_8));
        } else {
            return Files.newBufferedReader(indexPathsFile, StandardCharsets.UTF_8);
        }

    }

    @Field
    public void setIndexPathsFile(String path) {
        indexPathsFile = Paths.get(path);
    }

    @Field
    public void setIndexPathsUrls(List<String> urls) {
        indexPathsUrls = urls;
    }

    @Override
    public void checkInitialization(InitializableProblemHandler problemHandler)
            throws TikaConfigException {
        if (indexPathsUrls == null && indexPathsFile == null) {
            throw new TikaConfigException("must specify an indexPathsFile or an indexPathsUrl");
        }
        if (indexPathsFile != null && !Files.isRegularFile(indexPathsFile)) {
            throw new TikaConfigException("indexPathsFile must exist");
        }

        if (indexPathsUrls != null && indexPathsUrls.size() == 0) {
            throw new TikaConfigException("indexPathsUrls must not be empty");
        }

    }


    private void tryToAdd(String line) throws InterruptedException, TimeoutException {
        if (line.trim().endsWith(".gz")) {
            FetchEmitTuple t = new FetchEmitTuple(line, new FetchKey(getFetcherName(), line),
                    new EmitKey(getEmitterName(), line));
            tryToAdd(t);
        }
    }
}
