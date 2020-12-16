package org.tallison.ingest;

import org.apache.commons.io.IOUtils;
import org.tallison.quaerite.connectors.ESClient;
import org.tallison.quaerite.connectors.SearchClientFactory;
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
import java.util.List;

public class IngesterCLI {

    public static void main(String[] args) throws Exception {
        Connection pg = DriverManager.getConnection(args[0]);
        ESClient esClient = (ESClient) SearchClientFactory.getClient(args[1]);
        Path rootPath = Paths.get(args[2]);
        CompositeFeatureMapper compositeFeatureMapper = new CompositeFeatureMapper();
        String sql = getSelectStar();
        List<StoredDocument> docs = new ArrayList<>();
        try (Statement st = pg.createStatement()) {
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    StoredDocument sd = new StoredDocument(rs.getString(1));
                    compositeFeatureMapper.addFeatures(rs, rootPath, sd);
                    docs.add(sd);
                    if (docs.size() > 10) {
                        esClient.addDocuments(docs);
                        docs.clear();
                    }
                }
            }
        }
        esClient.addDocuments(docs);
        esClient.close();
    }

    private static String getSelectStar() throws IOException {
        return IOUtils.toString(
                IngesterCLI.class.getResourceAsStream("/selectStar.sql"),
                StandardCharsets.UTF_8.name());
    }
}
