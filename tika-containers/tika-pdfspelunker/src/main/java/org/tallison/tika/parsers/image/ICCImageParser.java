package org.tallison.tika.parsers.image;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.apache.tika.config.Field;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.EmbeddedDocumentUtil;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.image.ImageMetadataExtractor;
import org.apache.tika.parser.xmp.JempboxExtractor;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.utils.FileProcessResult;
import org.apache.tika.utils.ProcessUtils;

public class ICCImageParser extends AbstractParser {

    private static final Set<MediaType> TMP_SUPPORTED;
    private static Pattern INPUT_TOKEN_MATCHER = Pattern.compile("\\$\\{INPUT_FILE}");

    static {
        //see https://exiftool.org/forum/index.php?PHPSESSID=b4d0f99a69231020462aab1730256d77&topic=4081.msg19215#msg19215
        TMP_SUPPORTED = new HashSet<>(
                Arrays.asList(MediaType.image("jpeg"),
                        MediaType.image("jp2"),
                        MediaType.image("tif"),
                        MediaType.image("vnd.adobe.photoshop"),
                        MediaType.image("gif"),
                        MediaType.image("x-xcf"),
                        MediaType.image("x-raw-nikon")));
    }

    private static final Set<MediaType> SUPPORTED_TYPES =
            Collections.unmodifiableSet(TMP_SUPPORTED);
    private static Logger LOGGER = LoggerFactory.getLogger(ICCImageParser.class);

    private List<String> iccCommandLine;
    private long timeoutMs = 30000;

    private Path rootDir = Paths.get("/something/something");

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext parseContext) {
        return SUPPORTED_TYPES;
    }

    @Override
    public void parse(InputStream stream, ContentHandler contentHandler, Metadata metadata,
                      ParseContext parseContext) throws IOException, SAXException, TikaException {
        TemporaryResources tmp = new TemporaryResources();
        XHTMLContentHandler xhtml = new XHTMLContentHandler(contentHandler, metadata);
        xhtml.startDocument();
        try {
            TikaInputStream tis = TikaInputStream.get(stream, tmp);
            new ImageMetadataExtractor(metadata).parseJpeg(tis.getFile());
            new JempboxExtractor(metadata).parse(tis);
            tryIcc(tis, new BodyContentHandler(xhtml), metadata, parseContext);
        } finally {
            tmp.dispose();
        }
        xhtml.endDocument();
    }

    private void tryIcc(TikaInputStream tis, ContentHandler contentHandler,
                        Metadata metadata, ParseContext parseContext)
            throws IOException, SAXException {

        List<String> commandLine = buildIccCommandLine(tis.getPath());
        ProcessBuilder pb = new ProcessBuilder(commandLine);
        FileProcessResult result = ProcessUtils.execute(pb, timeoutMs, 1000, 1000);
        if (result.getExitValue() != 0) {
            //log problems
            return;
        }
        String stdout = result.getStdout();
        if (stdout.contains("0 output files created")) {
            //log
            return;
        }

        String iccPathString = tis.getPath().toAbsolutePath().toString();
        int i = iccPathString.lastIndexOf(".");
        if (i > -1) {
            iccPathString = iccPathString.substring(0, i);
        }
        iccPathString += ".icc";//specified on the commandline!
        Path iccPath = Paths.get(iccPathString);
        if (!Files.exists(iccPath)) {
            //log
            return;
        }
        EmbeddedDocumentExtractor ex =
                EmbeddedDocumentUtil.getEmbeddedDocumentExtractor(parseContext);
        try (TikaInputStream iccTis = TikaInputStream.get(iccPath)) {
            Path f = iccTis.getPath();
            String hex = "";
            try (InputStream is = Files.newInputStream(f)) {
                hex = DigestUtils.sha256Hex(is);
            }
            //temporary hack to extract icc bytes
            Path targ = rootDir.resolve(hex.substring(0,2)+"/"+hex.substring(2,4)+"/"+hex);
            if (Files.exists(targ)) {
                return;
            } else {
                Files.createDirectories(targ.getParent());
                Files.copy(f, targ, StandardCopyOption.REPLACE_EXISTING);
            }
            //ex.parseEmbedded(iccTis, contentHandler, new Metadata(), false);
        }
    }

    private List<String> buildIccCommandLine(Path p) {
        List<String> ret = new ArrayList<>();
        Matcher m = INPUT_TOKEN_MATCHER.matcher("");
        for (String line : iccCommandLine) {
            if (m.reset(line).find()) {
                line = m.replaceAll(ProcessUtils.escapeCommandLine(p.toAbsolutePath().toString()));
            }
            ret.add(line);
        }
        return ret;
    }

    @Field
    public void setIccCommandLine(List<String> commandLine) {
        this.iccCommandLine = commandLine;
    }

    @Field
    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }
}
