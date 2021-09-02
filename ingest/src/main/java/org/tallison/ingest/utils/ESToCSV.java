package org.tallison.ingest.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.tallison.quaerite.connectors.ESClient;
import org.tallison.quaerite.connectors.QueryRequest;
import org.tallison.quaerite.connectors.SearchClientFactory;
import org.tallison.quaerite.core.SearchResultSet;
import org.tallison.quaerite.core.StoredDocument;
import org.tallison.quaerite.core.queries.MatchAllDocsQuery;

public class ESToCSV {

    public static void main(String[] args) throws Exception {
        Path csv = Paths.get("/Users/allison/Desktop/eval-three-table.csv");
        String esString = "https://localhost:6443/file-observatory-eval-three-20210806";
        ESClient client = (ESClient) SearchClientFactory.getClient(esString);
        String[] cols = new String[]{
                "pinfo_producer",
                "pinfo_version",
                "universe",
                "universe_validity", "xpf_fonts_embedded", "tools_status",
                "tools_status_fail",
                "q_keys", "q_keys_and_values", "q_parent_and_keys"
        };
        List<String> fields = new ArrayList<>();
        Arrays.stream(cols).forEach(s -> fields.add(s));

        try (FileWriter writer = new FileWriter(csv.toFile(), StandardCharsets.UTF_8)) {
            CSVPrinter printer = new CSVPrinter(writer, CSVFormat.EXCEL);
            List<String> headers = new ArrayList<>();
            headers.add("id");
            for (String col : cols) {
                headers.add(col);
            }
            printer.printRecord(headers);
            QueryRequest queryRequest = new QueryRequest(new MatchAllDocsQuery());
            //this isn't working :(
            queryRequest.addFieldsToRetrieve(fields);
            SearchResultSet resultSet = client.startScroll(queryRequest, 1000, 5);
            int written = 0;
            int lastWritten = 0;
            String scrollId = resultSet.getScrollId();
            while (resultSet.size() > 0) {
                writeResults(cols, resultSet, printer);
                written += resultSet.size();
                lastWritten += resultSet.size();
                if (lastWritten > 10000) {
                    System.out.println("written: " + written);
                    lastWritten = 0;
  //                  break;
                }
                resultSet = client.scrollNext(scrollId, 5);
            }
        }
    }

    private static void writeResults(String[] cols, SearchResultSet resultSet, CSVPrinter printer)
            throws IOException {
        for (int i = 0; i < resultSet.size(); i++) {
            List<String> cells = new ArrayList<>();
            cells.add(resultSet.getId(i));
            StoredDocument storedDocument = resultSet.get(i);
            Map<String, Object> fields = storedDocument.getFields();
            for (String k : cols) {
                String val = "";
                Object fieldValue = fields.get(k);
                if (fieldValue == null) {
                    //do nothing
                } else if (fieldValue instanceof String) {
                    val = (String)fieldValue;
                } else if (fieldValue instanceof List){
                    StringBuilder sb = new StringBuilder();
                    List<String> fieldVals = (List)fieldValue;
                    int cnt = 0;
                    for (String v : fieldVals) {
                        if (cnt++ > 0) {
                            sb.append(" ");
                        }
                        sb.append(v);
                    }
                    val = sb.toString();
                }
                cells.add(val);
            }
            printer.printRecord(cells);
        }
    }
}
