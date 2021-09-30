package org.tallison.ingest.mappers;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PDF;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class TikaFeatureMapper implements FeatureMapper {

    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher, StoredDocument storedDocument) throws SQLException {
        addFromResultSet(row, storedDocument);
        addFromMetadataList(row.get(REL_PATH_KEY), fetcher, storedDocument);
    }

    private void addFromResultSet(Map<String, String> row, StoredDocument storedDocument) throws SQLException {
        storedDocument.addNonBlankField("tk_exit", row.get("tk_exit"));
    }

    private void addFromMetadataList(String relPath, Fetcher fetcher, StoredDocument storedDocument) {
        String k = "tika/" + relPath + ".json";
        try (InputStream is = fetcher.fetch(k, new Metadata())) {
            processJson(is, storedDocument);
        } catch (Exception e) {
            storedDocument.addNonBlankField("a_status", "missing");
        }
    }

    private void processJson(InputStream is, StoredDocument storedDocument) {

        List<Metadata> metadataList = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is,
                StandardCharsets.UTF_8))) {
            metadataList = JsonMetadataList.fromJson(reader);
        } catch (IOException e) {
            e.printStackTrace();
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
        //TODO -- add this in a better spot.
        storedDocument.addNonBlankField("tk_creator_tool", root.get(TikaCoreProperties.CREATOR_TOOL));
        storedDocument.addNonBlankField("tk_producer", root.get(PDF.DOC_INFO_PRODUCER));//fix
        if (! storedDocument.getFields().keySet().contains("tk_oov")) {
            storedDocument.addNonBlankField("tk_oov", root.get("tika-eval:oov"));
            storedDocument.addNonBlankField("tk_num_tokens", root.get("tika-eval:numTokens"));
            storedDocument.addNonBlankField("tk_lang_detected", root.get("tika-eval:lang"));
        }
        storedDocument.addNonBlankField("tk_created", root.get(TikaCoreProperties.CREATED));
        storedDocument.addNonBlankField("tk_modified", root.get(TikaCoreProperties.MODIFIED));
        String mimeDetailed = root.get(Metadata.CONTENT_TYPE);
        String mime = mimeDetailed;
        if (mimeDetailed != null) {
            int i = mimeDetailed.indexOf(";");
            if (i > -1) {
                mime = mimeDetailed.substring(0, i);
                storedDocument.addNonBlankField("tk_mime", mime);
            } else {
                storedDocument.addNonBlankField("tk_mime", mimeDetailed);
            }
        }
        storedDocument.addNonBlankField("tk_mime_detailed", root.get(Metadata.CONTENT_TYPE));
        storedDocument.addNonBlankField("tk_format", root.get(TikaCoreProperties.FORMAT));
        storedDocument.addNonBlankField("tk_title", root.get(TikaCoreProperties.TITLE));
        storedDocument.addNonBlankField("tk_subject", root.get(DublinCore.SUBJECT));

        storedDocument.addNonBlankField("tk_pdf_version", root.get(PDF.PDF_VERSION));
        storedDocument.addNonBlankField("tk_pdfa_version", root.get(PDF.PDFA_VERSION));
        storedDocument.addNonBlankField("tk_pdf_extension_version", root.get(PDF.PDF_EXTENSION_VERSION));
        storedDocument.addNonBlankField("tk_action_trigger", root.get(PDF.ACTION_TRIGGER));
        addUnmappedUnicodeChars(root, storedDocument);
    }

    private void addUnmappedUnicodeChars(Metadata m, StoredDocument sd) {
        int[] unmapped = m.getIntValues(PDF.UNMAPPED_UNICODE_CHARS_PER_PAGE);
        int[] chars = m.getIntValues(PDF.CHARACTERS_PER_PAGE);

        int unmappedTotal = 0;
        int charsTotal = 0;
        for (int c : unmapped) {
            unmappedTotal += c;
        }
        for (int c : chars) {
            charsTotal += c;
        }
        if (charsTotal > 0) {
            double unmappedPercent = (double)(unmappedTotal)/(double)(charsTotal);
            sd.addNonBlankField("tk_percent_unmapped_unicode", Double.toString(unmappedPercent));
        }
    }
}
