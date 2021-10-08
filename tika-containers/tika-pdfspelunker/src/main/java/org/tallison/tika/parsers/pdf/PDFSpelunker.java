package org.tallison.tika.parsers.pdf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSObject;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.EmbeddedDocumentUtil;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;

public class PDFSpelunker extends AbstractParser {

    private static final Tika TIKA = new Tika();
    private static Set<MediaType> SUPPORTED_TYPES =
            Collections.singleton(MediaType.application("pdf"));

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
        //process images first
        Set<COSStream> visitedStreams = new HashSet<>();
        AtomicInteger imageCounter = new AtomicInteger();
        for (PDPage page : pdDocument.getPages()) {
            ImageGraphicsEngine engine =
                    new ImageGraphicsEngine(page, embeddedDocumentExtractor, visitedStreams,
                            imageCounter, xhtml, metadata, parseContext);
            try {
                engine.run();
            } catch (IOException e) {
                EmbeddedDocumentUtil.recordException(e, metadata);
            }
        }

        Map<Long, COSObject> objects = new HashMap<>();
        for (COSObject cosObject : pdDocument.getDocument().getObjects()) {
            objects.put(cosObject.getObjectNumber(), cosObject);
        }
        Set<Long> streams = new HashSet<>();
        for (COSObject cosObject : pdDocument.getDocument().getObjects()) {
            COSBase base = cosObject.getObject();
            if (base instanceof COSStream) {
                streams.add(cosObject.getObjectNumber());
            } else if (base instanceof COSDictionary) {
                COSDictionary dict = (COSDictionary) base;
                for (COSName k : dict.keySet()) {
                    if (k == COSName.PARENT) {
                        continue;
                    }
                    processDictEntry(objects, k, dict.getItem(k).getCOSObject(), visitedStreams);
                }
            }
        }
        //now iterate through the unseen streams?
        //these seem to be submasks...do we care about these?
        for (Long id : streams) {
            COSObject cosObject = objects.get(id);
            COSStream stream = (COSStream) cosObject.getObject();
            if (visitedStreams.contains(stream)) {
                //note that we're suppressing softmasks
                continue;
            }
            String filterString = getFilterString(stream);
            String k = id + " -> " +
                    cosObject.getItem(COSName.TYPE) + " : " + cosObject.getItem(COSName.SUBTYPE) +
                            " :: " + filterString;
            processStream(k, stream);
        }

    }

    private String getFilterString(COSStream stream) {
        StringBuilder sb = new StringBuilder();
        COSBase base = stream.getFilters();
        if (base instanceof COSArray) {
            int i = 0;
            for (COSBase el : ((COSArray) base)) {
                if (el instanceof COSName) {
                    if (i > 0) {
                        sb.append("->");
                    }
                    sb.append(((COSName) el).getName());
                    i++;
                }
            }
        } else if (base instanceof COSName) {
            sb.append(((COSName) base).getName());
        }
        return sb.toString();
    }

    private void processDictEntry(Map<Long, COSObject> objects, COSName k, COSBase item,
                                  Set<COSStream> visitedStreams) {
        COSBase base = item;

        if (base instanceof COSObject) {
            COSBase value = ((COSObject) base).getObject();
            long objNumber = ((COSObject) base).getObjectNumber();

            if (value instanceof COSStream) {
                if (visitedStreams.contains((COSStream) value)) {
                    return;
                }
                processStream(k.getName(), (COSStream) value);

                visitedStreams.add((COSStream) value);
            } else if (value instanceof COSArray) {
                //     do something
            } else {
                //do something
            }
        } else if (base instanceof COSArray) {
            int i = 0;
            for (COSBase el : ((COSArray) base)) {
                if (el instanceof COSObject) {
                    long objNumber = ((COSObject) el).getObjectNumber();
                    COSBase obj = ((COSObject) el).getObject();
                    if (obj instanceof COSStream) {
                        if (visitedStreams.contains((COSStream) obj)) {
                            continue;
                        }

                        processStream(k.getName(), (COSStream) obj);
                        visitedStreams.add((COSStream) obj);
                    }
                }
            }
        }
    }

    private void processStream(String k, COSStream stream) {
        String mt = null;
        if (!k.equals("FontFile")) { //FontFile3
            //return;
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (InputStream is = stream.createInputStream()) {
            IOUtils.copy(is, bos);
        } catch (IOException e) {

        }
        try (InputStream is = TikaInputStream.get(bos.toByteArray())) {
            mt = TIKA.detect(is);
        } catch (IOException e) {

        }
        byte[] bytes = bos.toByteArray();
        int firstLen = Math.min(6, bytes.length);
        byte[] first = new byte[firstLen];
        System.arraycopy(bytes, 0, first, 0, firstLen);


        int len = Math.min(100, bytes.length);
        String content = new String(bytes, 0, len, StandardCharsets.UTF_8);
        System.out.println(
                "detected:  " + Hex.encodeHexString(first) +
                        " : " + mt + " :: " + k + " : " +
                        content.replaceAll("[\r\n]+", " "));
    }
}
