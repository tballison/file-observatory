package org.tallison.observatory.pdfjs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;

import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PDF;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.ToXMLContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;

public class PDFJSOutputParserTest {

    @Test
    public void testBasic() throws Exception {
        Metadata metadata = new Metadata();
        ContentHandler contentHandler = new ToXMLContentHandler();
        XHTMLContentHandler xhtmlContentHandler = new XHTMLContentHandler(contentHandler, metadata);
        xhtmlContentHandler.startDocument();
        Parser p = new PDFJSOutputParser();
        try (InputStream is = PDFJSOutputParserTest.class
                .getResourceAsStream("/test-documents/test-basic.txt")) {
            p.parse(is, xhtmlContentHandler, metadata, new ParseContext());
        }
        xhtmlContentHandler.endDocument();
        String content = contentHandler.toString();
        assertEquals("Microsoft® Word 2016", metadata.get(PDF.PRODUCER));
        assertTrue(content.contains("<div page=\"1\">here is some page 1 content"));
        assertTrue(content.contains("<div page=\"2\">some page 2 content"));
        assertEquals(4, metadata.getValues(PDFJSOutputParser.WARNINGS).length);
    }

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
        assertEquals("PScript5.dll Version 5.2.2", metadata.get(TikaCoreProperties.CREATOR_TOOL));
        assertEquals("18-956 Google LLC v. Oracle America, Inc. (04/05/2021)", metadata.get(TikaCoreProperties.TITLE));
    }
}
