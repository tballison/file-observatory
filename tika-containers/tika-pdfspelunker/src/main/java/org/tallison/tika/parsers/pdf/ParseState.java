package org.tallison.tika.parsers.pdf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pdfbox.cos.COSObject;
import org.apache.pdfbox.cos.COSObjectKey;
import org.apache.pdfbox.cos.COSStream;

import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;

class ParseState {
    final XHTMLContentHandler xhtml;
    final Metadata parentMetadata;
    final ParseContext parseContext;
    final Set<COSStream> visitedStreams;
    final EmbeddedDocumentExtractor embeddedDocumentExtractor;
    final AtomicInteger imageCounter;
    final Set<COSObjectKey> visitedObjects;
    final Map<COSObjectKey, String> xObjectCosPaths = new HashMap<>();
    List<String> cosPath;
    String type = null;
    String subtype = null;
    int maxCosPath = 0;

    public ParseState(XHTMLContentHandler xhtml, Metadata parentMetadata,
                      ParseContext parseContext, Set<COSStream> visitedStreams,
                      EmbeddedDocumentExtractor embeddedDocumentExtractor,
                      AtomicInteger imageCounter, Set<COSObjectKey> visitedObjects) {
        this.parentMetadata = parentMetadata;
        this.xhtml = xhtml;
        this.parseContext = parseContext;
        this.visitedStreams = visitedStreams;
        this.embeddedDocumentExtractor = embeddedDocumentExtractor;
        this.imageCounter = imageCounter;
        this.visitedObjects = visitedObjects;
    }

    void setCosPath(List<String> cosPath) {
        this.cosPath = cosPath;
        if (cosPath.size() > maxCosPath) {
            maxCosPath = cosPath.size();
        }
    }

    void setXObjectCosPath(COSObjectKey k, String cosPath) {
        xObjectCosPaths.put(k, cosPath);
    }

    String getCosPath(COSObjectKey k) {
        return xObjectCosPaths.get(k);
    }

}