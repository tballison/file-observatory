package org.tallison.tika.parsers.pdf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSObject;
import org.apache.pdfbox.cos.COSObjectKey;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.EmbeddedDocumentUtil;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.EmbeddedContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;

public class PDFSpelunker extends AbstractParser {

    public static Property COS_PATH = Property.externalText("pdfsp:cos-path");
    public static Property STREAM_FILTER_CHAIN = Property.externalText("pdfsp:stream-filter-chain");
    public static Property STREAM_TYPE = Property.externalText("pdfsp:stream-type");

    private static Set<MediaType> SUPPORTED_TYPES =
            Collections.singleton(MediaType.application("pdf"));
    private static Logger LOG = LoggerFactory.getLogger(PDFSpelunker.class);
    private static Set<String> DO_NOT_PARSE_EMBEDDED = Set.of("Contents", "ToUnicode",
            "Function", "Mask", "SMask", "XObject", "Fm1");

    AtomicInteger imageCounter = new AtomicInteger(0);

    Set<String> streamTypes = Set.of("ToUnicode", "FontFile1", "FontFile2", "FontFile3",
            "Font", "Fm1", "XObject", "Contents", "SMask", "Function", "ColorSpace",
            "DestOutputProfile", "Metadata");

    static Set<COSName> NEVER_FOLLOW = Set.of(COSName.PARENT, COSName.DEST, COSName.A);


    @Override
    public Set<MediaType> getSupportedTypes(ParseContext parseContext) {
        return SUPPORTED_TYPES;
    }

    @Override
    public void parse(InputStream inputStream, ContentHandler contentHandler, Metadata metadata,
                      ParseContext parseContext) throws IOException, SAXException, TikaException {
        XHTMLContentHandler xhtml = new XHTMLContentHandler(contentHandler, metadata);
        xhtml.startDocument();
        try (TemporaryResources tmp = new TemporaryResources()) {
            TikaInputStream tis = TikaInputStream.get(inputStream, tmp);
            try (PDDocument pdDocument = PDDocument.load(tis.getFile())) {
                processDocument(pdDocument, xhtml, metadata, parseContext);
            }
        }
        xhtml.endDocument();
    }

    private void processDocument(PDDocument pdDocument, XHTMLContentHandler xhtml,
                                 Metadata metadata, ParseContext parseContext) {
        EmbeddedDocumentExtractor embeddedDocumentExtractor =
                EmbeddedDocumentUtil.getEmbeddedDocumentExtractor(parseContext);

        Set<COSStream> visitedStreams = new HashSet<>();
        Set<COSObjectKey> visitedObjects = new HashSet<>();
        ParseState parseState = new ParseState(xhtml, metadata, parseContext, visitedStreams,
                embeddedDocumentExtractor, imageCounter, visitedObjects);

        List<String> cosPath = new ArrayList<>();
        cosPath.add("catalog");
        parseState.setCosPath(cosPath);
        //run through full document, but skip xobject streams (but not any of their children)
        processDict(null, pdDocument.getDocumentCatalog().getCOSObject(), parseState);
        cosPath.clear();
        cosPath.add("metadata");
        parseState.setCosPath(cosPath);
        processDict(null, pdDocument.getDocumentInformation().getCOSObject(), parseState);

        //now process xobject streams -- need the full processing of state, etc.
        //to get this right. :(
        for (PDPage page : pdDocument.getPages()) {
            ImageGraphicsEngine engine = new ImageGraphicsEngine(page, parseState);
            try {
                engine.run();
            } catch (IOException e) {
                LOG.warn("problem extracting images...");
            }
        }

        cosPath.clear();
        cosPath.add("orphans");
        parseState.setCosPath(cosPath);
        //look for orphans
        for (COSObject cosObject : pdDocument.getDocument().getObjects()) {
            COSBase base = cosObject.getObject();

            if (base instanceof COSStream) {
                if (!parseState.visitedStreams.contains((COSStream) base)) {
                    processStream(null, (COSStream) base, parseState, true);
                }
            }
        }
    }

    private void processDict(COSObjectKey key,
                             COSDictionary dict, ParseState parseState) {
        List<String> cosPath = new ArrayList<>();
        cosPath.addAll(parseState.cosPath);
        String oldType = parseState.type;
        String oldSubType = parseState.subtype;
        parseState.type = dict.getNameAsString(COSName.TYPE);
        parseState.subtype = dict.getNameAsString(COSName.SUBTYPE);

        for (Map.Entry<COSName, COSBase> e : dict.entrySet()) {
            if (NEVER_FOLLOW.contains(e.getKey())) {
                continue;
            }
            String n = e.getKey().getName();
            COSBase base = e.getValue();
            if (base instanceof COSObject) {
                COSObjectKey k = new COSObjectKey(((COSObject) base));
                if (parseState.visitedObjects.contains(k)) {
                    continue;
                }
                parseState.visitedObjects.add(k);
            }

            List<String> localPath = cloneAndAdd(n, cosPath);

            parseState.setCosPath(localPath);
            processBase(base, parseState);
        }
        parseState.setCosPath(cosPath);
        parseState.type = oldType;
        parseState.subtype = oldSubType;
    }

    private void processBase(COSBase base, ParseState parseState) {
        COSObjectKey key = null;
        if (base instanceof COSObject) {
            key = new COSObjectKey((COSObject) base);
            //TODO: check for already visited?
            base = ((COSObject) base).getObject();
        }

        if (base instanceof COSStream) {
            //cosstream is a cosdict, so test for cosstream first
            processStream(key, (COSStream) base, parseState, false);
        } else if (base instanceof COSDictionary) {
            processDict(key, (COSDictionary) base, parseState);
        } else if (base instanceof COSArray) {
            processArray(key, ((COSArray) base), parseState);
        }

    }

    private void processArray(COSObjectKey key, COSArray arr, ParseState parseState) {
        //check to see if this array is an array of streams that
        //should be concatenated
        int streamCount = 0;
        int interestingCount = 0;
        for (COSBase b : arr) {
            if (b instanceof COSStream) {
                streamCount++;
            } else if (b instanceof COSObject) {
                COSBase obj = ((COSObject) b).getObject();
                if (obj instanceof COSStream) {
                    streamCount++;
                } else if (obj instanceof COSArray){
                    interestingCount++;
                } else if (obj instanceof COSDictionary) {
                    interestingCount++;
                }
            } else if (b instanceof COSArray){
                interestingCount++;
            } else if (b instanceof COSDictionary) {
                interestingCount++;
            }
        }
        //short circuit if this is a list of, e.g. widths
        if (streamCount == 0 && interestingCount == 0) {
            return;
        }
        if (streamCount == arr.size()) {
            try {
                processStreamArray(key, arr, parseState);
            } catch (IOException e) {
                LOG.warn("problem concatenating stream array");
            }
            return;
        }
        int i = 0;
        String myKey = null;
        for (COSBase b : arr) {
            if (b instanceof COSObject) {
                COSObjectKey k = new COSObjectKey((COSObject) b);
                if (parseState.visitedObjects.contains(k)) {
                    continue;
                }
                parseState.visitedObjects.add(k);
                myKey = k.toString();
            }
            String arrKey = "ARR_"+i;
            parseState.cosPath.add("ARR_"+i);
            processBase(b, parseState);
            String removed = parseState.cosPath.remove(parseState.cosPath.size()-1);
            if (! removed.equals(arrKey)) {
                //warn
            }
            i++;
        }
    }

    private void processStreamArray(COSObjectKey key, COSArray arr, ParseState parseState) throws IOException {
        COSStream concatenated = new COSStream();
        try (OutputStream os = concatenated.createOutputStream()) {
            for (COSBase stream : arr) {
                if (stream instanceof COSObject) {
                    stream = ((COSObject)stream).getObject();
                    parseState.visitedStreams.add((COSStream) stream);
                }
                try (InputStream is = ((COSStream) stream).createInputStream()) {
                    IOUtils.copy(is, os);
                }
            }
        }
        processStream(key, concatenated, parseState, false);
    }

    private List<String> cloneAndAdd(String n, List<String> cosPath) {
        List<String> ret = new ArrayList<>();
        ret.addAll(cosPath);
        ret.add(n);
        return ret;
    }

    private void processStream(COSObjectKey key, COSStream stream, ParseState parseState,
                               boolean isOrphan) {
        if (parseState.visitedStreams.contains(stream)) {
            return;
        }

        if (COSName.XOBJECT.equals(stream.getCOSName(COSName.TYPE)) &&
                ! COSName.FORM.equals(stream.getCOSName(COSName.SUBTYPE))) {
                //skip xobjects... they'll be picked up by the ImageGraphicsEngine
                if (key != null) {
                    parseState.setXObjectCosPath(key, getPathString(parseState.cosPath));
                }

        } else {
            parseState.visitedStreams.add(stream);
        }

        String embeddedType = guessEmbeddedType(parseState, isOrphan);
        /*System.out.println("guessed type " + parseState.type + " " +
                parseState.subtype + " :: " + embeddedType);*/
        if (! DO_NOT_PARSE_EMBEDDED.contains(embeddedType)) {

            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            try (InputStream is = stream.createInputStream()) {
                IOUtils.copy(is, bos);
            } catch (IOException e) {
                //log
                LOG.warn("IOException on initial stream read; trying raw stream");
                bos = new ByteArrayOutputStream();
                try (InputStream is = stream.createRawInputStream()) {
                    IOUtils.copy(is, bos);
                } catch (IOException innerEx) {
                    LOG.warn("IOException reading raw inputstream");
                    //add these to metadata
                    return;
                }
            }
            Metadata metadata = new Metadata();

            metadata.set(STREAM_TYPE, embeddedType);
            metadata.set(COS_PATH, getPathString(parseState.cosPath));
            metadata.set(STREAM_FILTER_CHAIN, getFilterString(getFilters(stream)));

            if (parseState.embeddedDocumentExtractor.shouldParseEmbedded(metadata)) {

                try (InputStream embeddedIs = TikaInputStream.get(bos.toByteArray())) {
                    parseState.embeddedDocumentExtractor.parseEmbedded(embeddedIs, new EmbeddedContentHandler(parseState.xhtml), metadata, false);
                } catch (SAXException | IOException e) {
                    LOG.warn("Exception parsing", e);
                    //add these to metadata
                }
            }
        }

        //now see if there are any other streams under this stream
        processDict(key, (COSDictionary) stream, parseState);
    }

    private String guessEmbeddedType(ParseState parseState, boolean isOrphaned) {
        if (isOrphaned) {
            return "unknown-orphan";
        }
        for (int i = parseState.cosPath.size()-1; i > -1; i--) {
            if (streamTypes.contains(parseState.cosPath.get(i))) {
                return parseState.cosPath.get(i);
            }
        }
        return "UNKNOWN";
    }

    public static String getFilterString(List<COSName> filters) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (COSName n : filters) {
            if (i > 0) {
                sb.append("->");
            }
            sb.append(n.getName());
            i++;
        }
        return sb.toString();
    }

    public static String getPathString(List<String> items) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String n : items) {
            if (i > 0) {
                sb.append("->");
            }
            sb.append(n);
            i++;
        }
        return sb.toString();
    }

    public static List<COSName> getFilters(COSStream cosStream) {
        List<COSName> ret = new ArrayList<>();
        COSBase base = cosStream.getFilters();
        if (base == null) {
            return ret;
        }
        if (base instanceof COSObject) {
            base = ((COSObject)base).getObject();
        }
        if (base == null) {
            return ret;
        }
        if (base instanceof COSName) {
            ret.add((COSName) base);
            return ret;
        } else if (base instanceof COSArray) {
            for (COSBase item : (COSArray)base) {
                if (item instanceof COSObject) {
                    item = ((COSObject)base).getObject();
                }
                if (item == null) {
                    continue;
                }
                if (item instanceof COSName) {
                    ret.add((COSName)item);
                } else {
                    LOG.warn("trying to find cosname, but see: {}", item.getClass());
                }
            }
        } else {
            LOG.warn("trying to find filter, expecting name or array, but see: {}",
                    base.getClass());
        }
        return ret;
    }
}
