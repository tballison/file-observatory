package org.tallison.ingest.mappers;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PDF;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class TikaFeatureMapper implements FeatureMapper {

    @Override
    public void addFeatures(ResultSet resultSet, Path rootDir, StoredDocument storedDocument) throws SQLException {
        addFromResultSet(resultSet, storedDocument);
        addFromMetadataList(resultSet.getString(1), rootDir, storedDocument);
    }

    private void addFromResultSet(ResultSet resultSet, StoredDocument storedDocument) throws SQLException {
        int exit = resultSet.getInt("tk_exit");
        storedDocument.addNonBlankField("tk_exit", Integer.toString(exit));
    }

    private void addFromMetadataList(String relPath, Path rootDir, StoredDocument storedDocument) {
        Path p = rootDir.resolve("tika/json/"+relPath+".json");
        if (! Files.isRegularFile(p)) {
            //log
            return;
        }
        List<Metadata> metadataList = null;
        try (BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
            metadataList = JsonMetadataList.fromJson(reader);
        } catch (IOException| TikaException e) {
            //log
            return;
        }
        if (metadataList.size() == 0) {
            //log
            return;
        }
        int allAttachments = metadataList.size()-1;
        long inlineAttachments = metadataList.stream().filter( m ->
                TikaCoreProperties.EmbeddedResourceType.INLINE.toString()
                        .equals(m.get(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE))).count();

        long macros = metadataList.stream().filter( m ->
                TikaCoreProperties.EmbeddedResourceType.MACRO.toString()
                        .equals(m.get(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE))).count();
        long attachments = allAttachments-inlineAttachments-macros;
        storedDocument.addNonBlankField("tk_num_attachments",
                Long.toString(attachments));
        storedDocument.addNonBlankField("tk_num_macros",
                Long.toString(macros));

        Metadata root = metadataList.get(0);
        storedDocument.addNonBlankField("tk_creator_tool", root.get(TikaCoreProperties.CREATOR_TOOL));
        storedDocument.addNonBlankField("tk_producer", root.get(PDF.DOC_INFO_PRODUCER));//fix
        storedDocument.addNonBlankField("tk_oov", root.get("tika-eval:oov"));
        storedDocument.addNonBlankField("tk_num_tokens", root.get("tika-eval:numTokens"));
        storedDocument.addNonBlankField("sha256", root.get("X-TIKA:digest:SHA256"));
    }
}
