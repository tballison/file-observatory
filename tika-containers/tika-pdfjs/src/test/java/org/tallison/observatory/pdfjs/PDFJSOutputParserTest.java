package org.tallison.observatory.pdfjs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;

import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PDF;
import org.apache.tika.metadata.PagedText;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.ToXMLContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;

public class PDFJSOutputParserTest {

    @Test
    public void testXMP() throws Exception {
        Metadata metadata = new Metadata();
        ContentHandler contentHandler = new ToXMLContentHandler();
        XHTMLContentHandler xhtmlContentHandler = new XHTMLContentHandler(contentHandler, metadata);
        xhtmlContentHandler.startDocument();
        Parser p = new PDFJSOutputParser();
        try (InputStream is = PDFJSOutputParserTest.class
                .getResourceAsStream("/test-documents/test-xmp.txt")) {
            p.parse(is, xhtmlContentHandler, metadata, new ParseContext());
        }
        xhtmlContentHandler.endDocument();
        String content = contentHandler.toString();
        assertEquals(2, (int)metadata.getInt(PagedText.N_PAGES));
        assertEquals("PScript5.dll Version 5.2.2", metadata.get(TikaCoreProperties.CREATOR_TOOL));
        assertEquals("18-956 Google LLC v. Oracle America, Inc. (04/05/2021)", metadata.get(TikaCoreProperties.TITLE));
        assertEquals("TT: CALL empty stack (or invalid entry).", metadata.getValues(PDFJSOutputParser.INFOS)[0]);
        assertTrue(metadata.getValues(PDFJSOutputParser.WARNINGS)[0].contains(
                "fetchStandardFontData: failed to fetch file \"FoxitSerifBold.pfb\""));

        assertTrue(content.contains("page 1 content"));
        assertTrue(content.contains("page 2 content"));
    }

    @Test
    public void testXMPEmbedded() throws Exception {
        Metadata metadata = new Metadata();
        ContentHandler contentHandler = new ToXMLContentHandler();
        XHTMLContentHandler xhtml = new XHTMLContentHandler(contentHandler, metadata);
        xhtml.startDocument();
        Parser p = new PDFJSOutputParser();
        try (InputStream is = PDFJSOutputParserTest.class
                .getResourceAsStream("/test-documents/test-xmp2.txt")) {
            p.parse(is, xhtml, metadata, new ParseContext());
        }
        xhtml.endDocument();
        String content = contentHandler.toString();
        assertEquals(1, (int)metadata.getInt(PagedText.N_PAGES));
        assertTrue(Boolean.parseBoolean(metadata.get(PDF.HAS_ACROFORM_FIELDS)));
    }
}
