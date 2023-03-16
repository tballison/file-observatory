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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.junit.Test;
import org.tallison.cc.index.CCIndexRecord;

public class CCIndexRecordTest {
    @Test
    public void testParse() throws Exception {
        String row = "au,org,eric)/wp-content/uploads/2022/10/eric_someone-elses-shoes_yp.pdf 20221203023659 {\"url\": \"https://eric.org.au/wp-content/uploads/2022/10/ERIC_SOMEONE-ELSES-SHOES_YP.pdf\", " +
                "\"mime\": \"text/html\", \"mime-detected\": \"text/html\", " +
                "\"status\": \"302\", \"digest\": \"3Y6666M7MDHCUFTSC2DZG773FI7ZXU5O\", " +
                "\"length\": \"1030\", \"offset\": \"7574516\", \"filename\": " +
                "\"crawl-data/CC-MAIN-2022-49/segments/1669446710918.58/crawldiagnostics/CC-MAIN-20221203011523-20221203041523-00732.warc.gz\", \"redirect\":" +
                " \"http://eric.org.au/login/}\"}";

        row = "ca,honda,motorcycle)/model/sport/cbr500r/{{socialitemurl}} 20221201104753 {\"url\": \"https://motorcycle.honda.ca/model/sport/cbr500r/%7B%7BsocialItemUrl%7D%7D\", \"mime\": \"text/html\", \"mime-detected\": \"text/html\", \"status\": \"302\", \"digest\": \"A6BPQ4AVJSMSJQEVYOA3TAOD3LNX67OJ\", \"length\": \"1552\", \"offset\": \"11370174\", \"filename\": \"crawl-data/CC-MAIN-2022-49/segments/1669446710808.72/crawldiagnostics/CC-MAIN-20221201085558-20221201115558-00805.warc.gz\", \"redirect\": \"/Error/404?error=https://motorcycle.honda.ca/model/sport/cbr500r/{{socialItemUrl}}\"}";
        List<CCIndexRecord> records = CCIndexRecord.parseRecords(row);
        System.out.println(records.get(0));
    }

    @Test
    public void testFileParse() throws Exception {
        Path p = Paths.get("/Users/allison/data/cc/index-work/cdx-00000.gz");
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(p)), StandardCharsets.UTF_8))) {
            String line = reader.readLine();
            while (line != null) {
                List<CCIndexRecord> records = CCIndexRecord.parseRecords(line);
                line = reader.readLine();
            }
        }
    }
}
