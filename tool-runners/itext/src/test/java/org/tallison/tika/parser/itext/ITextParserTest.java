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


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.xml.sax.ContentHandler;

import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.ToXMLContentHandler;

public class ITextParserTest {

    @Test
    @Disabled("needs to set license path as a property")
    public void testBasic() throws Exception {
        ContentHandler handler = new ToXMLContentHandler();
        Metadata metadata = new Metadata();
        try (TikaInputStream tis = TikaInputStream.get(
                this.getClass().getResourceAsStream(
                        "/test-documents/testPDF.pdf"))) {
            Parser p = new AutoDetectParser();
            ParseContext parseContext = new ParseContext();
            p.parse(tis, handler, metadata, parseContext);
        }
        assertTrue(handler.toString().contains("Apache Tika is a toolkit"));
        assertEquals("true", metadata.get("itext:rebuilt-xref"));
        assertEquals("1", metadata.get("xmpTPg:NPages"));
    }

    @Test
    public void oneOff() throws Exception {
        Path path = Paths.get("/Users/allison/Desktop/tmp");
        path = path.resolve("d61c39d4468dcfc83a8d6c69c26c5f78d9596e83e2cf31af63c30be7cb8b8873");
        ContentHandler handler = new ToXMLContentHandler();
        Metadata metadata = new Metadata();
        try (TikaInputStream tis = TikaInputStream.get(path)) {
            Parser p = new AutoDetectParser();
            ParseContext parseContext = new ParseContext();
            p.parse(tis, handler, metadata, parseContext);
        }
        System.out.println(handler.toString());
    }
}
