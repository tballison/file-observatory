package org.tallison.pdfutils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class PDFVersionator {

    private static final byte[] EOF = "%%EOF".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PDF = "%PDF-".getBytes(StandardCharsets.US_ASCII);
    private static long MAX_PREFIX = 10000;
    /**
     * This looks for %%EOF in PDF files and copies out the different versions
     * to the target dir.  It does _not_ print out the last %%EOF.
     *
     * @param file
     * @param dir
     * @throws IOException
     */
    public static void dumpVersions(Path file, Path dir) throws IOException {
        List<Long> eofs = getEOFs(file);
        if (eofs.size() <= 1) {
            return;
        }
        if (! Files.isDirectory(dir)) {
            Files.createDirectories(dir);
        }
        for (int i = 0; i < eofs.size()-1; i++) {
            Path out = dir.resolve("version-"+i+".pdf");
            try (InputStream is = Files.newInputStream(file);
                 OutputStream os = Files.newOutputStream(out)) {
                IOUtils.copyLarge(is, os, 0, eofs.get(i));
            }
        }
    }

    public static List<Long> getEOFs(Path file) throws IOException {

        List<Long> eofs = new ArrayList<>();
        try (InputStream is = Files.newInputStream(file)) {
            StreamSearcher streamSearcher = new StreamSearcher(EOF);
            long base = 0;
            long eof = streamSearcher.search(is);
            while (eof > -1) {
                eofs.add(base+eof);
                base += eof;
                eof = streamSearcher.search(is);
            }
        }
        return eofs;
    }

    public static Pair<Long, byte[]> getHeader(Path path) throws IOException {
        long offset = -1;
        try (InputStream is = Files.newInputStream(path)) {
            StreamSearcher streamSearcher = new StreamSearcher(PDF);
            long base = 0;
            offset = streamSearcher.search(is);
        }
        if (offset == -1) {
            return Pair.of(-1l, new byte[0]);
        }
        offset -= PDF.length;
        if (offset <= 0) {
            return Pair.of(offset, new byte[0]);
        }
        int len = (int)Math.min(offset, MAX_PREFIX);
        byte[] pre = new byte[len];
        try (InputStream is = Files.newInputStream(path)) {
            IOUtils.readFully(is, pre);
            return Pair.of(offset, pre);
        }
    }
}
