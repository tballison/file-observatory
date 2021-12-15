package org.tallison.observatory.pdfjs;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.ExternalProcess;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PDF;
import org.apache.tika.metadata.PagedText;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.utils.StringUtils;

public class PDFJSOutputParser extends AbstractParser {

    public static final Property WARNINGS = Property.externalTextBag("PDFJS:Warnings");
    public static final Property INFOS = Property.externalTextBag("PDFJS:Infos");

    private static final Pattern RANDOM_KEY_PATTERN = Pattern.compile("# Random Key: (\\d+)");
    private static final String DOCUMENT_LOADED = "# Document Loaded";

    private static final String WARNING_PREFIX = "Warning: ";
    private static final String INFO_PREFIX = "Info: ";

    private static final String TEXT_CONTENT = "## Text Content";
    private static final String END_OF_DOCUMENT = "# End of Document";

    private static final String PDDOC_INFO = "## Info";
    private static final String XMP_METADATA = "## Metadata";

    private static final String PAGE_START = "# Page";

    //skip an info line that logs the time to extrat content per page
    private static final String SKIP_INFO = "getTextContent: time=";

    private static Pattern PAGE_PATTERN = Pattern.compile("# Page (\\d+)");
    private static Pattern NUMBER_OF_PAGES_PATTERN = Pattern.compile("# Number of Pages: (\\d+)");

    private static Map<String, Property> PROPERTIES = new ConcurrentHashMap();
    static {
        PROPERTIES.put("Creator", PDF.DOC_INFO_CREATOR_TOOL);
        PROPERTIES.put("xmp:creatortool", TikaCoreProperties.CREATOR_TOOL);
        PROPERTIES.put("dc:creator", TikaCoreProperties.CREATOR);
        PROPERTIES.put("Producer", PDF.PRODUCER);
        PROPERTIES.put("PDFFormatVersion", PDF.PDF_VERSION);
        //PROPERTIES.put("IsLinearized", )
        PROPERTIES.put("IsAcroFormPresent", PDF.HAS_ACROFORM_FIELDS);
        PROPERTIES.put("IsXFAPreset", PDF.HAS_XFA);
        PROPERTIES.put("IsCollectionPresent", PDF.HAS_COLLECTION);
        PROPERTIES.put("CreationDate", TikaCoreProperties.CREATED);
        PROPERTIES.put("xmp:createdate", TikaCoreProperties.CREATED);
        PROPERTIES.put("ModDate", TikaCoreProperties.MODIFIED);
        PROPERTIES.put("xmp:moddifydate", TikaCoreProperties.MODIFIED);
        PROPERTIES.put("Title", TikaCoreProperties.TITLE);
        PROPERTIES.put("dc:title", TikaCoreProperties.TITLE);
//        PROPERTIES.put("IsSignaturesPresent", PDF.HAS_SIGNATURE);
    }
    @Override
    public Set<MediaType> getSupportedTypes(ParseContext parseContext) {
        return Collections.emptySet();
    }

    @Override
    public void parse(InputStream inputStream, ContentHandler contentHandler, Metadata metadata,
                      ParseContext parseContext) throws IOException, SAXException, TikaException {
        Set<String> infos = new LinkedHashSet<>();
        Set<String> warnings = new LinkedHashSet<>();
        Matcher randomKeyMatcher = RANDOM_KEY_PATTERN.matcher("");
        Matcher numPagesMatcher = NUMBER_OF_PAGES_PATTERN.matcher("");
        Matcher pageMatcher = PAGE_PATTERN.matcher("");
        String error = metadata.get(ExternalProcess.STD_ERR);
        if (!StringUtils.isBlank(error)) {
            return;
        }
        XHTMLContentHandler xhtml = new XHTMLContentHandler(contentHandler, metadata);
        /*
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream,
                StandardCharsets.UTF_8))) {
            String line = reader.readLine();
            while (line != null) {
                xhtml.characters(line);
                xhtml.characters("\n");
                line = reader.readLine();
            }
        }*/

        long randKey = -1;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream,
                StandardCharsets.UTF_8))) {
            String line = reader.readLine();
            while (line != null) {
                if (randKey < 0) {
                    randomKeyMatcher.reset(line);
                    if (randomKeyMatcher.find()) {
                        randKey = Long.parseLong(randomKeyMatcher.group(1));
                    }
                } else if (line.startsWith(PDDOC_INFO) && verify(randKey, PDDOC_INFO, line)) {
                    line = loadInfo(reader, randKey, numPagesMatcher, pageMatcher, metadata);
                    continue;
                } else if (line.startsWith(XMP_METADATA) && verify(randKey, XMP_METADATA, line)) {
                    line = loadInfo(reader, randKey, numPagesMatcher, pageMatcher, metadata);
                    continue;
                } else if (numPagesMatcher.reset(line).find() &&
                        verify(randKey, numPagesMatcher.group(0), line)) {
                    int numPages = Integer.parseInt(numPagesMatcher.group(1));
                    metadata.set(PagedText.N_PAGES, numPages);
                } else if (pageMatcher.reset(line).find() &&
                        verify(randKey, pageMatcher.group(0), line)) {
                    String pageNumber = pageMatcher.group(1);
                    xhtml.startElement("div", "page", pageNumber);
                    //this line is probably the start of the next page or end of document
                    line = loadPage(reader, randKey, xhtml, infos, warnings);
                    xhtml.endElement("div");
                    continue;
                } else if (line.startsWith(END_OF_DOCUMENT) && verify(randKey, END_OF_DOCUMENT, line)) {
                    break;
                }
                line = reader.readLine();
            }
        } finally {
            //xhtml.endDocument();
        }

        for (String info : infos) {
            metadata.add(INFOS, info);
        }
        for (String w : warnings) {
            metadata.add(WARNINGS, w);
        }
    }

    private boolean verify(long randKey, String prefix, String line) {
        //TODO prefix+ key=randKey should equal the line
        return line.trim().endsWith(" key="+randKey);
    }

    private String loadPage(BufferedReader reader, long randKey, XHTMLContentHandler xhtml,
                            Set<String> infos, Set<String> warnings) throws IOException,
            SAXException {
        String line = reader.readLine();
        boolean inText = false;
        while (line != null) {
            if (line.startsWith(END_OF_DOCUMENT) && verify(randKey, END_OF_DOCUMENT, line)) {
                return line;
            } else if (line.startsWith(PAGE_START) && verify(randKey, PAGE_START, line)) {
                return line;
            } else if (line.startsWith(WARNING_PREFIX)) {
                String w = line.substring(WARNING_PREFIX.length());
                warnings.add(w);
            } else if (line.startsWith(INFO_PREFIX)) {
                String info = line.substring(INFO_PREFIX.length());
                if (! info.contains(SKIP_INFO)) {
                    infos.add(info);
                }
            } else if (line.startsWith(TEXT_CONTENT) && verify(randKey, TEXT_CONTENT, line)) {
                return readContent(reader, randKey, xhtml);
            }
            line = reader.readLine();
        }
        throw new EOFException();
    }

    private String readContent(BufferedReader reader, long randKey,
                               XHTMLContentHandler xhtml) throws IOException, SAXException {
        String line = reader.readLine();
        while (line != null) {
            if (line.startsWith(END_OF_DOCUMENT) && verify(randKey, END_OF_DOCUMENT, line)) {
                return line;
            } else if (line.startsWith(PAGE_START) && verify(randKey, PAGE_START, line)) {
                return line;
            }
            xhtml.characters(line);
            xhtml.characters("\n");
            line = reader.readLine();
        }
        throw new EOFException();
    }

    private String loadInfo(BufferedReader reader, long randKey,
                            Matcher numPagesMatcher, Matcher pageMatcher,
                            Metadata metadata) throws IOException {
        String line = reader.readLine();
        StringBuilder sb = new StringBuilder();
        boolean inJson = false;
        while (line != null) {
            if (line.trim().equals("{")) {
                inJson = true;
            } else if (line.startsWith(PDDOC_INFO) && verify(randKey, PDDOC_INFO, line)) {
                parseJsonMetadata(sb.toString(), metadata);
                return line;
            } else if (line.startsWith(XMP_METADATA) && verify(randKey, XMP_METADATA, line)) {
                parseJsonMetadata(sb.toString(), metadata);
                return line;
            } else if (numPagesMatcher.reset(line).find() &&
                verify(randKey, numPagesMatcher.group(0), line)) {
                parseJsonMetadata(sb.toString(), metadata);
                return line;
            } else if (pageMatcher.reset(line).find() &&
                    verify(randKey, pageMatcher.group(0), line)) {
                parseJsonMetadata(sb.toString(), metadata);
                return line;
            } else if (line.startsWith(END_OF_DOCUMENT) && verify(randKey, END_OF_DOCUMENT, line)) {
                parseJsonMetadata(sb.toString(), metadata);
                return line;
            }
            if (inJson) {
                sb.append(line).append("\n");
            }
            line = reader.readLine();
        }
        if (line == null) {
            throw new EOFException();
        }
        return line;
    }

    private void parseJsonMetadata(String json, Metadata metadata) throws IOException {
        try {
            JsonNode root = new ObjectMapper().readTree(json);
            if (!root.isObject()) {
                return;
            }

            for (Iterator<String> it = root.fieldNames(); it.hasNext(); ) {
                String k = it.next();
                JsonNode v = root.get(k);
                writeMetadata(k, v, metadata);
            }
        } catch (IOException e) {
            throw e;
//            e.printStackTrace();
        }
    }

    private void writeMetadata(String k, JsonNode v, Metadata metadata) {
        if (! PROPERTIES.containsKey(k)) {
            return;
        }
        if (v.isBoolean()) {
            metadata.set(PROPERTIES.get(k), v.booleanValue());
        } else if (v.isArray()) {
            for (int i = 0; i < v.size(); i++) {
                metadata.add(PROPERTIES.get(k), v.get(i).asText());
            }
        } else {
            metadata.set(PROPERTIES.get(k), v.asText());
        }
    }
}
