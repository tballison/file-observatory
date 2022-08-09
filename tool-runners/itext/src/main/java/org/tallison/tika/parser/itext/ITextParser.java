/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.tika.parser.itext;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfReader;
import com.itextpdf.kernel.pdf.canvas.parser.PdfTextExtractor;
import com.itextpdf.kernel.pdf.canvas.parser.listener.SimpleTextExtractionStrategy;
import com.itextpdf.licensing.base.LicenseKey;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;

/**
 * Simple wrapper around iText for text extraction.
 *
 * This does not: extract metadata, embedded files, embedded images
 * or run OCR on rendered pages, etc.
 *
 * This is really a quite simple proof of concept.
 */
public class ITextParser extends AbstractParser {

    private static Set<MediaType> SUPPORTED_TYPES = Set.of(MediaType.application("pdf"));

    static {
        String licensePath = System.getProperty("itext-license");
        if (licensePath == null) {
            throw new RuntimeException("need property: 'itext-license'");
        }
        LicenseKey.loadLicenseFile(new File(licensePath));
    }

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext parseContext) {
        return SUPPORTED_TYPES;
    }

    @Override
    public void parse(InputStream inputStream, ContentHandler contentHandler, Metadata metadata,
                      ParseContext parseContext) throws IOException, SAXException, TikaException {
        TikaInputStream tis = TikaInputStream.cast(inputStream);
        TemporaryResources tmp = new TemporaryResources();
        if (tis == null) {
            tis = TikaInputStream.get(inputStream, tmp);
            tis.getPath();
        }
        XHTMLContentHandler xhtml = new XHTMLContentHandler(contentHandler, metadata);
        xhtml.startDocument();
        try (PdfReader reader = new PdfReader(tis.getFile())) {

            reader.setStrictnessLevel(PdfReader.StrictnessLevel.LENIENT);
            try (PdfDocument pdfDocument = new PdfDocument(reader)) {
                for (int i = 1; i <= pdfDocument.getNumberOfPages(); i++) {
                    String txt = PdfTextExtractor.getTextFromPage(pdfDocument.getPage(i),
                            new SimpleTextExtractionStrategy());
                    xhtml.startElement("div", "page", Integer.toString(i));
                    xhtml.characters(txt);
                    xhtml.endElement("div");
                }
            }
        } finally {
            tmp.close();
            xhtml.endDocument();
        }
    }
}
