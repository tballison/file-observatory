/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.tika.config.Initializable;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.http.HttpFetcher;

@Ignore
public class FetcherTest {
    @Test
    public void testOneOff() throws Exception {
        Fetcher f = new HttpFetcher();
        ((Initializable)f).initialize(Collections.EMPTY_MAP);
        String k = "https://direitosculturais.com.br/pdf.php?id=151";
        Path output = Paths.get("SPECIFY SOMETHING");
        try (InputStream is = f.fetch(k, new Metadata())) {
            Files.copy(is, output);
        }
    }

    @Test
    public void testBasic() throws Exception {
        String url = "https://direitosculturais.com.br/pdf.php?id=151";
        //url = "https://www.whatismybrowser.com/detect/what-http-headers-is-my-browser-sending";
        //url = "http://forexcopier.net/FT5-QuickStartGuideJp.pdf";
        url = "https://meglervisning.no/salgsoppgave/hent?instid=MSRCOPS&estateid=906638e5-7bc9-45b8-9eac-c77308ec4682";
        url = "https://irma.nps.gov/DataStore/DownloadFile/583612";
        HttpClient client = HttpClients.custom().create().build();
        HttpGet get = new HttpGet(url);

        //get.setHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) " +
         //       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36");
        HttpResponse r = client.execute(get);
        for (Header h : r.getAllHeaders()) {
            System.out.println(h);
        }
        System.out.println("content-length " + r.getFirstHeader("Content-Length"));
        System.out.println("LENGTH: " + r.getEntity().getContentLength());
        System.out.println("chunked: " + r.getEntity().isChunked());
        System.out.println("repeatable: " + r.getEntity().isRepeatable());
        System.out.println("streaming: " + r.getEntity().isStreaming());
        System.out.println(r.getStatusLine());
        Path output = Paths.get("/Users/allison/Desktop/tmp2.pdf");
       // try (OutputStream os = Files.newOutputStream(output)) {
            Files.copy(r.getEntity().getContent(), output, StandardCopyOption.REPLACE_EXISTING);
         //   r.getEntity().writeTo(os);
       // }
    }
}
