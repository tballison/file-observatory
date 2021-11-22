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
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PDF;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;

public class PDFJSOutputParser extends AbstractParser {

    public static final Property WARNINGS = Property.externalTextBag("PDFJS:Warnings");

    private static final String DOCUMENT_LOADED = "# Document Loaded";
    private static final String WARNING_PREFIX = "Warning: ";
    private static final String TEXT_CONTENT = "## Text Content";
    private static final String END_OF_DOCUMENT = "# End of Document";

    private static final String PDDOC_INFO = "## Info";
    private static final String XMP_METADATA = "## Metadata";

    private static final String PAGE_START = "# Page ";

    private static Pattern PAGE_PATTERN = Pattern.compile("# Page (\\d+)");
    private static Pattern JSON_HACK_MATCHER =
            Pattern.compile("\"([^\"]+)\": (?:\"([^\r\n]+)\"|([^\r\n]+))");


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
        PROPERTIES.put("ModDate", TikaCoreProperties.MODIFIED);
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
        Set<String> warnings = new HashSet<>();
        Matcher pageMatcher = PAGE_PATTERN.matcher("");
        XHTMLContentHandler xhtml = new XHTMLContentHandler(contentHandler, metadata);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream,
                StandardCharsets.UTF_8))) {
            String line = reader.readLine();
            while (line != null) {
                if (line.startsWith(PDDOC_INFO)) {
                    loadInfo(reader, metadata);
                } else if (line.startsWith(XMP_METADATA)) {
                    loadInfo(reader, metadata);
                } else if (pageMatcher.reset(line).find()) {
                    String pageNumber = pageMatcher.group(1);
                    xhtml.startElement("div", "page", pageNumber);
                    line = loadPage(reader, xhtml, warnings);
                    xhtml.endElement("div");
                    continue;
                } else if (line.startsWith(END_OF_DOCUMENT)) {
                    break;
                }
                line = reader.readLine();
            }
        } finally {
            //xhtml.endDocument();
        }
        for (String w : warnings) {
            metadata.add(WARNINGS, w);
        }
    }

    private String loadPage(BufferedReader reader, XHTMLContentHandler xhtml,
                            Set<String> warnings) throws IOException, SAXException {
        String line = reader.readLine();
        boolean inText = false;
        while (line != null) {
            if (line.startsWith(END_OF_DOCUMENT)) {
                return line;
            } else if (line.startsWith(PAGE_START)) {
                return line;
            } else if (line.startsWith(WARNING_PREFIX)) {
                String w = line.substring(WARNING_PREFIX.length());
                warnings.add(w);
            } else if (line.startsWith(TEXT_CONTENT)) {
                inText = true;
                line = reader.readLine();
                continue;
            } else if (inText) {
                xhtml.characters(line);
                xhtml.characters("\n");
            }

            line = reader.readLine();
        }
        throw new EOFException();
    }

    private void loadInfo(BufferedReader reader, Metadata metadata) throws IOException {
        String line = reader.readLine();
        Matcher jsonHack = JSON_HACK_MATCHER.matcher(line);
        StringBuilder sb = new StringBuilder();
        boolean inJson = false;
        while (line != null) {
            if (line.trim().equals("{")) {
                inJson = true;
            } else if (line.trim().equals("}")) {
                sb.append(line);
                parseJsonMetadata(sb.toString(), metadata);
                return;
            }
            if (inJson) {
                sb.append(line).append("\n");
            }
            line = reader.readLine();
        }
        if (line == null) {
            throw new EOFException();
        }
    }

    private void parseJsonMetadata(String json, Metadata metadata) {
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
            e.printStackTrace();
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
