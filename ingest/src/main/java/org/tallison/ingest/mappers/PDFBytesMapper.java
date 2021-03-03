package org.tallison.ingest.mappers;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.commons.codec.binary.Base64;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.ingest.FeatureMapper;
import org.tallison.quaerite.core.StoredDocument;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PDFBytesMapper implements FeatureMapper {

    private static Logger LOGGER = LoggerFactory.getLogger(PDFBytesMapper.class);


    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher, StoredDocument storedDocument)
            throws SQLException {
        boolean timeout = false;
        int exit = -1;
        // {"preheader":"","eofs":[931,1241],"header-offset":0}
        String json = row.get("pdfbytes_stdout");
        JsonObject root = JsonParser.parseReader(new StringReader(json)).getAsJsonObject();

        String preheader = "";
        if (root.has("preheader")) {
            String preheaderBase64 = root.get("preheader").getAsString();
            byte[] bytes = Base64.decodeBase64(preheaderBase64);
            preheader = new String(bytes, StandardCharsets.US_ASCII);
        }
        String headerOffset = root.get("header-offset").getAsString();
        List<String> eofs = new ArrayList<>();
        for (JsonElement el: root.getAsJsonArray("eofs")){
            eofs.add(el.getAsString());
        }
        storedDocument.addNonBlankField("b_eofs", eofs);
        storedDocument.addNonBlankField("b_num_eofs", Integer.toString(eofs.size()));
        storedDocument.addNonBlankField("b_preheader",
                ESUtil.stripIllegalUnicode(preheader));
        storedDocument.addNonBlankField("b_header_offset", headerOffset);

    }
}
