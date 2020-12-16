package org.tallison.tika.parsers.pdftotext;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.utils.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.ProcessExecutor;
import org.tallison.batchlite.writer.JDBCMetadataWriter;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class PDFToText implements Parser {

    private static final MediaType MEDIA_TYPE = MediaType.application("pdf");

    private static final Set<MediaType> SUPPORTED_TYPES =
            Collections.singleton(MEDIA_TYPE);

    private static final long TIMEOUT_MILLIS_DEFAULT = 60000;

    private static Logger LOGGER = LoggerFactory.getLogger(PDFToText.class);

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
                    "pdftotext",
                    ProcessUtils.escapeCommandLine(path.toAbsolutePath().toString()),
                    ProcessUtils.escapeCommandLine(output.toAbsolutePath().toString())
            };
            metadata.set(Metadata.CONTENT_LENGTH, Long.toString(Files.size(path)));

            ProcessBuilder pb = new ProcessBuilder(args);
            FileProcessResult processResult = ProcessExecutor.execute(pb, timeoutMillis,
                    1000, 1000);
            XHTMLContentHandler xhtml = new XHTMLContentHandler(contentHandler, metadata);
            xhtml.startDocument();
            readFile(output, xhtml);
            xhtml.endDocument();

            if (processResult.isTimeout()) {
                throw new TikaException("timeout", new TimeoutException());
            } else if (processResult.getExitValue() != 0) {
                throw new TikaException("Bad exit value: "+processResult.getExitValue() +
                        " :: "+processResult.getStderr());
            }
        }
    }

    private void readFile(Path output, XHTMLContentHandler xhtml) throws IOException, SAXException {

        if (! Files.isRegularFile(output)) {
            LOGGER.debug("not a file: "+output.toAbsolutePath().toString());
            return;
        }
        StringBuilder sb = new StringBuilder();
        boolean lastWasEmpty = false;
        try (BufferedReader reader = Files.newBufferedReader(output, StandardCharsets.UTF_8)) {
            String line = reader.readLine();
            while (line != null) {
                if (lastWasEmpty && line.trim().length() == 0) {
                    xhtml.startElement("p");
                    xhtml.characters(sb.toString());
                    xhtml.endElement("p");
                    sb.setLength(0);
                }
                if (line.trim().length() == 0) {
                    lastWasEmpty = true;
                } else {
                    lastWasEmpty = false;
                }
                sb.append(line).append("\n");
                line = reader.readLine();
            }
            String last = sb.toString();
            if (last.trim().length() > 0) {
                xhtml.startElement("p");
                xhtml.characters(last);
                xhtml.endElement("p");
            }
        }
    }
}
