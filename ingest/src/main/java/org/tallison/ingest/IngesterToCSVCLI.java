package org.tallison.ingest;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.IOUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;

import org.tallison.ingest.mappers.ProfileFeatureMapper;
import org.tallison.ingest.mappers.QPDFFeatureMapper;
import org.tallison.ingest.mappers.TikaFeatureMapper;
import org.tallison.quaerite.connectors.ESClient;
import org.tallison.quaerite.core.StoredDocument;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IngesterToCSVCLI {
    //single threaded for now
    public static void main(String[] args) throws Exception {
        Connection pg = DriverManager.getConnection(args[0]);
        ESClient esClient = null;//(ESClient) SearchClientFactory.getClient(args[1]);
        Fetcher fetcher = FetcherManager.load(Paths.get(args[2])).getFetcher(
                "file-obs-fetcher");
        Path outPath = Paths.get(args[3]);

        List<FeatureMapper> mappers = new ArrayList<>();
        mappers.add(new TikaFeatureMapper());
        mappers.add(new QPDFFeatureMapper());
        mappers.add(new ProfileFeatureMapper());
        CompositeFeatureMapper compositeFeatureMapper = new CompositeFeatureMapper(mappers);
        String sql = getSelectStar();
        List<StoredDocument> docs = new ArrayList<>();
//        CSVPrinter printer = CSVFormat.EXCEL.print(outPath, StandardCharsets.UTF_8);
        String[] cols = new String[]{
                "fname", "shasum_256", "collection", "tk_mime",
                "tk_producer", "tk_creator_tool", "q_keys", "q_keys_oos",
                "q_parent_and_keys", "q_keys_and_values"
        };


        try (Statement st = pg.createStatement()) {
            try (ResultSet rs = st.executeQuery(sql);
                 CSVPrinter printer = CSVFormat.EXCEL.print(outPath, StandardCharsets.UTF_8)) {
                printer.printRecord(cols);
                int rowCount = 0;
                while (rs.next()) {
                    Map<String, String> dbRow = IngesterCLI.mapify(rs);
                    StoredDocument sd = new StoredDocument(rs.getString(1));
                    compositeFeatureMapper.addFeatures(dbRow, fetcher, sd);
                    Map<String, String> row = new HashMap<>();
                    for (Map.Entry<String, Object> e : sd.getFields().entrySet()) {
                        if (e.getValue() instanceof String) {
                            String val = (String)e.getValue();
                            row.put(e.getKey(), val);
                        } else if (e.getValue() instanceof ArrayList) {
                            ArrayList<String> list = (ArrayList)e.getValue();
                            Collections.sort(list);
                            StringBuilder sb = new StringBuilder();
                            int i = 0;
                            for (String value : list) {
                                if (i++ > 0) {
                                    sb.append(" ");
                                }
                                sb.append(value);
                            }
                            row.put(e.getKey(), sb.toString());
                        } else {
                            System.out.println("not a string "+e.getKey() + " : "+e.getValue().getClass());
                        }
                    }
                    List<String> out = new ArrayList<>();
                    for (String k : cols) {
                        if (row.containsKey(k)) {
                            String val = row.get(k);
                            if (val.length() > 10000) {
                                val = val.substring(0, 10000);
                                //System.out.println("truncating: "+val);
                            }
                            out.add(val);
                        } else {
                            out.add("");
                        }
                    }
                    System.out.println("rows: "+rowCount++);
                    printer.printRecord(out);
                    /*
                    docs.add(sd);
                    if (docs.size() > 10) {
                        esClient.addDocuments(docs);
                        docs.clear();
                    }*/
                }
            }
        }
        //esClient.addDocuments(docs);
        //esClient.close();
    }

    private static String getSelectStar() throws IOException {
        return IOUtils.toString(
                IngesterToCSVCLI.class.getResourceAsStream("/selectStar-lite.sql"),
                StandardCharsets.UTF_8.name());
    }
}
