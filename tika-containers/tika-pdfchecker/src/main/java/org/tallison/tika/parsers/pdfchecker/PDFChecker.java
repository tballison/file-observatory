package org.tallison.tika.parsers.pdfchecker;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.tika.config.Field;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.utils.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.ProcessExecutor;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class PDFChecker extends AbstractParser {

    private static final MediaType MEDIA_TYPE = MediaType.application("pdf");

    private static final Set<MediaType> SUPPORTED_TYPES =
            Collections.singleton(MEDIA_TYPE);

    private static final long TIMEOUT_MILLIS_DEFAULT = 60000;

    private static Logger LOGGER = LoggerFactory.getLogger(PDFChecker.class);

    private long timeoutMillis = TIMEOUT_MILLIS_DEFAULT;

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext parseContext) {
        return SUPPORTED_TYPES;
    }

    @Override
    public void parse(InputStream inputStream, ContentHandler contentHandler,
                      Metadata metadata, ParseContext parseContext) throws IOException, SAXException, TikaException {
        try (TemporaryResources tmp = new TemporaryResources()) {
            Path path = TikaInputStream.get(inputStream, tmp).getPath();
            Path output = tmp.createTempFile();
            String[] args = new String[]{
                    "/pdfchecker-bin/PDF_Checker/pdfchecker",
                    "-j",
                    "/pdfchecker-bin/PDF_Checker/CheckerProfiles/everything.json",
                    "-i",
                    ProcessUtils.escapeCommandLine(path.toAbsolutePath().toString()),
                    "-s",
                    ProcessUtils.escapeCommandLine(output.toAbsolutePath().toString())
            };
            metadata.set(Metadata.CONTENT_LENGTH, Long.toString(Files.size(path)));

            ProcessBuilder pb = new ProcessBuilder(args);
            FileProcessResult processResult = ProcessExecutor.execute(pb, timeoutMillis,
                    1000, 1000);
            XHTMLContentHandler xhtml = new XHTMLContentHandler(contentHandler, metadata);
            xhtml.startDocument();
            readFile(output, metadata);
            xhtml.endDocument();

            if (processResult.isTimeout()) {
                throw new TikaException("timeout", new TimeoutException());
            } else if (processResult.getExitValue() != 0) {
                throw new TikaException("Bad exit value: "+processResult.getExitValue() +
                        " :: "+processResult.getStderr());
            }
        }
    }

    @Field
    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    private void readFile(Path p, Metadata metadata) throws IOException, TikaException, SAXException {

        if (!Files.isRegularFile(p)) {
            LOGGER.debug("not a file: " + p.toAbsolutePath().toString());
            throw new TikaException("Missing output file");
        }
        try (Reader reader = Files.newBufferedReader(p)) {
            JsonElement rootElement = JsonParser.parseReader(reader);
            if (rootElement.isJsonNull()) {
                //log
                return;
            }
            JsonObject root = rootElement.getAsJsonObject();
            StringBuilder sb = new StringBuilder();
            if (root.has("analysis-summary")) {

                JsonObject summary = root.getAsJsonObject("analysis-summary");
                if (summary.has("can-be-optimized")) {
                    boolean canBeOptimized = summary.getAsJsonPrimitive("can-be-optimized").getAsBoolean();
                    if (canBeOptimized) {
                        sb.append("can-be-optimized").append(" ");
                    }
                }
                if (summary.has("information")) {
                    JsonArray info = summary.getAsJsonArray("information");
                    for (JsonElement el : info) {
                        sb.append(el.getAsString()).append(" ");
                    }
                    metadata.set("pc_summary_info", sb.toString().trim());
                }
                if (summary.has("errors")) {
                    for (JsonElement el : summary.getAsJsonArray("errors")) {
                        metadata.add("pc_summary_errors", el.getAsString());
                    }
                }
            }
            if (root.has("fonts-results")) {
                JsonObject fontsResults = root.getAsJsonObject("fonts-results");
                if (fontsResults.has("errors")) {
                    JsonObject fontErrors = fontsResults.getAsJsonObject("errors");
                    List<String> fontErrorKeys = new ArrayList<>();
                    for (String n : fontErrors.keySet()) {
                        metadata.add("pc_font_errors", n);
                    }
                }
            }
        }
    }
}
