package org.tallison.tika.parsers.pdf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSObject;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.filter.Filter;
import org.apache.pdfbox.filter.FilterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PDFImageStreamUtil {

    private static Logger LOG = LoggerFactory.getLogger(PDFImageStreamUtil.class);

    private static final List<String> JPEG =
            Arrays.asList(COSName.DCT_DECODE.getName(), COSName.DCT_DECODE_ABBREVIATION.getName());


    private static final List<String> JP2 = Collections.singletonList(COSName.JPX_DECODE.getName());

    private static final List<String> JB2 = Collections.singletonList(COSName.JBIG2_DECODE.getName());


    public static InputStream createImageInputStream(COSStream cosStream) throws IOException {
        List<COSName> filters = PDFSpelunker.getFilters(cosStream);
        String suffix = getSuffix(filters);
        List<String> stopFilters = new ArrayList<>();
        if ("jpg".equals(suffix)) {
            stopFilters = JPEG;
        } else if ("jp2".equals(suffix)) {
            stopFilters = JP2;
        } else if ("jb2".equals(suffix)) {
            stopFilters = JB2;
        }
        System.out.println("image suffix "+ suffix);
        return createInputStream(cosStream, filters, stopFilters);
    }

    private static InputStream createInputStream(COSStream cosStream, List<COSName> filters,
                                                 List<String> stopFilters) throws IOException {
        InputStream is = cosStream.createRawInputStream();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        if (filters != null) {
            for(int i = 0; i < filters.size(); ++i) {
                COSName nextFilter = (COSName)filters.get(i);
                if (stopFilters != null && stopFilters.contains(nextFilter.getName())) {
                    break;
                }

                Filter filter = FilterFactory.INSTANCE.getFilter(nextFilter);

                try {
                    filter.decode((InputStream)is, os, cosStream, i);
                } finally {
                    org.apache.pdfbox.io.IOUtils.closeQuietly((Closeable)is);
                }

                is = new ByteArrayInputStream(os.toByteArray());
                os.reset();
            }
        }

        return (InputStream)is;
    }

    private static String getSuffix(List<COSName> filters) {

        if (filters == null) {
            return "png";
        } else if (filters.contains(COSName.DCT_DECODE)) {
            return "jpg";
        } else if (filters.contains(COSName.JPX_DECODE)) {
            return "jpx";
        } else if (filters.contains(COSName.CCITTFAX_DECODE)) {
            return "tif";
        } else if (filters.contains(COSName.FLATE_DECODE) || filters.contains(COSName.LZW_DECODE) ||
                filters.contains(COSName.RUN_LENGTH_DECODE)) {
            return "png";
        } else if (filters.contains(COSName.JBIG2_DECODE)) {
            return "jb2";
        } else {
            LOG.warn("getSuffix() returns null, filters: " + filters);
            return null;
        }
    }

}
