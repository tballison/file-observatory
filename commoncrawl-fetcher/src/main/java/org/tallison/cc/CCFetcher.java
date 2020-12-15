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

package org.tallison.cc;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.RedirectLocations;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.log4j.Logger;
import org.netpreserve.jwarc.HttpParser;
import org.netpreserve.jwarc.MediaType;
import org.netpreserve.jwarc.WarcPayload;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import org.netpreserve.jwarc.WarcResponse;
import org.tallison.cc.index.CCIndexRecord;


import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

/**
 * Class to read in an index file or a subset of an index file
 * and to "get" those files from cc to a local directory
 *
 * This relies heavily on centic9's CommonCrawlDocumenDownload.
 * Thank you, Dominik!!!
 */
public class CCFetcher {

    enum FETCH_STATUS {
        BAD_URL, //0
        FETCHED_IO_EXCEPTION,//1
        FETCHED_NOT_200,//2
        FETCHED_IO_EXCEPTION_READING_ENTITY,//3
        FETCHED_IO_EXCEPTION_SHA1,//4
        ALREADY_IN_REPOSITORY,//5
        FETCHED_EXCEPTION_COPYING_TO_REPOSITORY,//6
        ADDED_TO_REPOSITORY; //7
    }

    private final static String AWS_BASE = "https://commoncrawl.s3.amazonaws.com/";
    static Logger LOGGER = Logger.getLogger(CCFetcher.class);

    private Base32 base32 = new Base32();


    private void execute(Connection connection,
                         Path rootDir, boolean cleanStart, int max) throws SQLException, IOException {
        connection.setAutoCommit(false);
        createFetchTable(connection, cleanStart);
        PreparedStatement insert = prepareInsert(connection);

        String sql = getSelectStar();
        int rows = 0;
        HttpClient httpClient = HttpClients.createDefault();

        try (Statement st = connection.createStatement()) {
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    processRow(httpClient, rs, insert, rootDir);
                    rows++;
                    if (rows % 10000 == 0) {
                        insert.executeBatch();
                    }
                    if (rows % 100 == 0) {
                        LOGGER.info("fetched " + rows + " files");
                    }
                    //should add limit command to sql
                    if (rows >= max) {
                        break;
                    }
                }
            }
        }
        insert.executeBatch();
        connection.commit();
    }

    private String getSelectStar() throws IOException {

        List<String> lines = IOUtils.readLines(
                this.getClass().getResourceAsStream(
                        "/selectFilesToFetchFromCC.sql"),
                StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(line);
            sb.append("\n");
        }
        return sb.toString();
    }

    private PreparedStatement prepareInsert(Connection connection) throws SQLException {
        String sql = "insert into cc_fetch values (?, ?, ?, ?)";
        return connection.prepareStatement(sql);
    }

    private void createFetchTable(Connection connection, boolean cleanStart) throws SQLException  {

        String sql = "select * from cc_fetch limit 1";
        if (! cleanStart) {
            //test to see if the table already exists
            boolean createTable = false;
            try (Statement st = connection.createStatement()) {
                try (ResultSet rs = st.executeQuery(sql)) {
                    while (rs.next()) {

                    }
                }
            } catch (SQLException e) {
                //table doesn't exist
                createTable = true;
            }
            if (!createTable) {
                return;
            }
        }
        try (Statement st = connection.createStatement()) {
            sql = "drop table if exists cc_fetch";
            st.execute(sql);

            sql = "create table cc_fetch (" +
                    "id integer, " +
                    "status_id int, "+
                    "fetched_digest varchar(64), " +
                    "fetched_length bigint);";
            st.execute(sql);

            sql = "drop table if exists cc_fetch_status";
            st.execute(sql);

            sql = "create table cc_fetch_status " +
                    "(id integer primary key, status varchar(64));";
            st.execute(sql);


            for (FETCH_STATUS status : FETCH_STATUS.values()) {

                sql = "insert into cc_fetch_status values (" +
                        status.ordinal() + ",'" + status.name() + "');";
                st.execute(sql);
            }
        }
        connection.commit();
    }

    private void processRow(HttpClient httpClient, ResultSet rs, PreparedStatement insert, Path rootDir)
            throws IOException, SQLException {
        CCIndexRecord record = new CCIndexRecord();
        int id = rs.getInt("id");
        record.setDigest(rs.getString("cc_index_digest"));
        record.setFilename(rs.getString("warc_file_name"));
        record.setOffset(rs.getInt("warc_offset"));
        record.setLength(rs.getInt("warc_length"));

        fetch(id, record, httpClient, insert, rootDir);
    }

    private void fetch(int id, CCIndexRecord r,
                       HttpClient httpClient, PreparedStatement insert, Path rootDir)
            throws SQLException, IOException {

        Path targFile = rootDir.resolve(r.getDigest().substring(0, 2) + "/" + r.getDigest());

        if (Files.isRegularFile(targFile)) {
            writeStatus(id, FETCH_STATUS.ALREADY_IN_REPOSITORY, insert);
            LOGGER.info("already retrieved:"+targFile.toAbsolutePath());
            return;
        }

        String url = AWS_BASE+r.getFilename();
        URI uri = null;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            LOGGER.warn("Bad url: " + url);
            writeStatus(id, FETCH_STATUS.BAD_URL, insert);
            return;
        }
        HttpHost target = new HttpHost(uri.getHost());
        String urlPath = uri.getRawPath();
        if (uri.getRawQuery() != null) {
            urlPath += "?" + uri.getRawQuery();
        }
        HttpGet httpGet = null;
        try {
            httpGet = new HttpGet(urlPath);
        } catch (Exception e) {
            LOGGER.warn("bad path " + uri.toString(), e);
            writeStatus(id, FETCH_STATUS.BAD_URL, insert);
            return;
        }
        httpGet.addHeader("Range", r.getOffsetHeader());
        HttpCoreContext coreContext = new HttpCoreContext();
        HttpResponse httpResponse = null;
        URI lastURI = null;
        try {
            httpResponse = httpClient.execute(target, httpGet, coreContext);
            RedirectLocations redirectLocations = (RedirectLocations) coreContext.getAttribute(
                    DefaultRedirectStrategy.REDIRECT_LOCATIONS);
            if (redirectLocations != null) {
                for (URI redirectURI : redirectLocations.getAll()) {
                    lastURI = redirectURI;
                }
            } else {
                lastURI = httpGet.getURI();
            }
        } catch (IOException e) {
            LOGGER.warn("IOException for " + uri.toString(), e);
            writeStatus(id, FETCH_STATUS.FETCHED_IO_EXCEPTION, insert);
            return;
        }
        //lastURI = uri.resolve(lastURI);

        if (httpResponse.getStatusLine().getStatusCode() != 200 && httpResponse.getStatusLine().getStatusCode() != 206) {
            LOGGER.warn("Bad status for " + uri.toString() + " : " + httpResponse.getStatusLine().getStatusCode());
            writeStatus(id, FETCH_STATUS.FETCHED_NOT_200, insert);
            return;
        }
        Path tmp = null;
        boolean isTruncated = false;
        try {
            //this among other parts is plagiarized from centic9's CommonCrawlDocumentDownload
            //probably saved me hours.  Thank you, Dominik!
            tmp = Files.createTempFile("cc-getter", "");
            try (InputStream is = new GZIPInputStream(httpResponse.getEntity().getContent())) {
                WarcReader warcreader = new WarcReader(is);
                int i = 0;
                //should be a single warc per file
                for (WarcRecord record : warcreader) {
                    if (i++ == 0) {
                        if (record instanceof WarcResponse && record.contentType().base().equals(MediaType.HTTP)) {
                            Optional<WarcPayload> payload = ((WarcResponse) record).payload();
                            if (payload.isPresent()) {
                                Files.copy(payload.get().body().stream(),
                                        tmp,
                                        StandardCopyOption.REPLACE_EXISTING);
                            } else {
                                LOGGER.warn("payload not present ?! id=" + id);
                            }
                        } else {
                            LOGGER.warn("not a warc response ?! id=" + id);
                        }
                    } else {
                        LOGGER.warn("more than one record ?! id=" + id);
                    }

                }


            }
        } catch (IOException e) {
            LOGGER.warn("problem parsing warc file", e);
            writeStatus(id, FETCH_STATUS.FETCHED_IO_EXCEPTION_READING_ENTITY, insert);
            deleteTmp(tmp);
            return;
        }

        String digest = null;
        long tmpLength = 0l;
        try (InputStream is = Files.newInputStream(tmp)) {
            digest = base32.encodeAsString(DigestUtils.sha1(is));
            tmpLength = Files.size(tmp);
        } catch (IOException e) {
            writeStatus(id,FETCH_STATUS.FETCHED_IO_EXCEPTION_SHA1, insert);
            LOGGER.warn("IOException during digesting: " + tmp.toAbsolutePath());
            deleteTmp(tmp);
            return;
        }


        try {
            Files.createDirectories(targFile.getParent());
            Files.copy(tmp, targFile);
        } catch (IOException e) {
            writeStatus(id, FETCH_STATUS.FETCHED_EXCEPTION_COPYING_TO_REPOSITORY, insert);
            deleteTmp(tmp);
        }
        writeStatus(id, FETCH_STATUS.ADDED_TO_REPOSITORY, digest, tmpLength, insert);
        deleteTmp(tmp);
    }

    private void writeStatus(int id, FETCH_STATUS status, String digest, long length, PreparedStatement insert) throws SQLException {
        insert.setInt(1, id);
        insert.setInt(2, status.ordinal());
        insert.setString(3, digest);
        insert.setLong(4, length);
        insert.addBatch();
    }

    private void writeStatus(int id, FETCH_STATUS status, PreparedStatement insert) throws SQLException {
        insert.setInt(1, id);
        insert.setInt(2, status.ordinal());
        insert.setNull(3, Types.VARCHAR);
        insert.setNull(4, Types.BIGINT);
        insert.addBatch();
    }


    static String clean(String s) {
        //make sure that the string doesn't contain \t or new line
        if (s == null) {
            return "";
        }

        if (s.startsWith("\"")) {
            s = s.substring(1);
        }
        if (s.endsWith("\"")) {
            s = s.substring(0,s.length()-1);
        }
        if (s.contains("\"")) {
            s = "\""+s.replaceAll("\"", "\"\"")+"\"";
        }
        return s.replaceAll("\\s", " ");
    }

    private void deleteTmp(Path tmp) {
        try {
            Files.delete(tmp);
        } catch (IOException e1) {
            LOGGER.error("Couldn't delete tmp file: " + tmp.toAbsolutePath());
        }
    }

    private static Options getOptions() {
        Options options = new Options();

        options.addRequiredOption("j", "jdbc", true,
                "jdbc connection string");
        options.addRequiredOption("o", "outputDir", true,
                "directory to which to write the literal files");
        options.addOption("m", "max", true, "max files to retrieve");
        options.addOption("c", "cleanStart", false,
                "whether or not to delete the cc_fetch and " +
                        "cc_fetch_status tables (default = false)");
        return options;
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser cliParser = new DefaultParser();
        CommandLine line = cliParser.parse(getOptions(), args);
        Connection connection = DriverManager.getConnection(line.getOptionValue("j"));
        Path outputDir = Paths.get(line.getOptionValue("o"));
        int max = -1;
        if (line.hasOption("m")) {
            max = Integer.parseInt(line.getOptionValue("m"));
        }
        CCFetcher ccFetcher = new CCFetcher();
        boolean cleanStart = false;
        if (line.hasOption("c")) {
            cleanStart = true;
        }
        ccFetcher.execute(connection, outputDir, cleanStart, max);
    }
}

