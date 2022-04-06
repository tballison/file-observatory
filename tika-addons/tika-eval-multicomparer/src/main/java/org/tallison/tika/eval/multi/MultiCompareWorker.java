package org.tallison.tika.eval.multi;

import static org.apache.tika.eval.core.metadata.TikaEvalMetadataFilter.LANGUAGE;
import static org.apache.tika.eval.core.metadata.TikaEvalMetadataFilter.LANGUAGE_CONFIDENCE;
import static org.apache.tika.eval.core.metadata.TikaEvalMetadataFilter.NUM_ALPHA_TOKENS;
import static org.apache.tika.eval.core.metadata.TikaEvalMetadataFilter.NUM_TOKENS;
import static org.apache.tika.eval.core.metadata.TikaEvalMetadataFilter.NUM_UNIQUE_ALPHA_TOKENS;
import static org.apache.tika.eval.core.metadata.TikaEvalMetadataFilter.NUM_UNIQUE_TOKENS;
import static org.apache.tika.eval.core.metadata.TikaEvalMetadataFilter.OUT_OF_VOCABULARY;
import static org.tallison.tika.eval.multi.MultiComparerCLI.EXTRACT_STATUS.OK;
import static org.tallison.tika.eval.multi.MultiComparerCLI.EXTRACT_STATUS.TOO_LONG;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tika.eval.core.langid.LanguageIDWrapper;
import org.apache.tika.eval.core.textstats.BasicTokenCountStatsCalculator;
import org.apache.tika.eval.core.textstats.CommonTokens;
import org.apache.tika.eval.core.textstats.CompositeTextStatsCalculator;
import org.apache.tika.eval.core.textstats.TextStatsCalculator;
import org.apache.tika.eval.core.tokens.CommonTokenResult;
import org.apache.tika.eval.core.tokens.ContrastStatistics;
import org.apache.tika.eval.core.tokens.TokenContraster;
import org.apache.tika.eval.core.tokens.TokenCounts;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.language.detect.LanguageResult;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.fetcher.FetcherManager;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

public class MultiCompareWorker implements Callable<Integer> {
    private static final Property EXTRACT_STATUS =
            Property.externalInteger("multi-compare:extract-status");
    public static final Property NUM_COMMON_TOKENS = Property.externalInteger("tika-eval" +
            ":num_common_tokens");
    private static final Property[] EVAL_PROPS = new Property[] {
            NUM_TOKENS, NUM_ALPHA_TOKENS, NUM_COMMON_TOKENS, LANGUAGE, OUT_OF_VOCABULARY,
    };

    private static Logger LOGGER = LoggerFactory.getLogger(MultiCompareWorker.class);

    private static final AtomicLong TUPLES_PROCESSED = new AtomicLong(0);
    private final Connection connection;
    private final ArrayBlockingQueue<FetchEmitTuple> tuples;
    private final FetcherManager fetcherManager;
    private final List<String> tools;
    private final List<String> extensions;
    private PreparedStatement insert;
    private final TokenContraster tokenContraster = new TokenContraster();
    private final long maxInputStreamLength;
    static CompositeTextStatsCalculator TEXT_STATS_CALCULATOR;

    static {
        List<TextStatsCalculator> calcs = new ArrayList<>();
        calcs.add(new BasicTokenCountStatsCalculator());
        calcs.add(new CommonTokens());
        TEXT_STATS_CALCULATOR = new CompositeTextStatsCalculator(calcs);
    }

    public MultiCompareWorker(String jdbcConnection, ArrayBlockingQueue<FetchEmitTuple> tuples,
                              FetcherManager fetcherManager, List<String> tools,
                              List<String> extensions, long maxInputStreamLength) throws
            SQLException {
        this.connection = DriverManager.getConnection(jdbcConnection);
        this.tuples = tuples;
        this.fetcherManager = fetcherManager;
        this.tools = tools;
        this.extensions = extensions;
        this.maxInputStreamLength = maxInputStreamLength;
    }

    @Override
    public Integer call() throws Exception {
        insert = prepareInsert();
        try {
            while (true) {
                FetchEmitTuple t = tuples.take();
                if (t == PipesIterator.COMPLETED_SEMAPHORE) {
                    LOGGER.info("completed; about to offer 'stop' semaphore");
                    tuples.offer(t);
                    LOGGER.info("completed; offered'stop' semaphore");
                    return 1;
                }
                process(t);
                long processed = TUPLES_PROCESSED.incrementAndGet();
                if (processed % 100 == 0) {
                    LOGGER.info("processed {}", processed);
                }
            }
        } finally {
            connection.close();
        }
    }

    private void process(FetchEmitTuple t) throws TikaException, SQLException {
        List<Metadata> metadatas = new ArrayList<>();
        for (int i = 0; i < tools.size(); i++) {
            load(tools.get(i), t.getFetchKey().getFetchKey(), extensions.get(i), metadatas);
        }
        List<TokenCounts> tokenCounts = new ArrayList<>();
        for (Metadata m : metadatas) {
            TokenCounts tc = filter(m);
            tokenCounts.add(tc);
        }

        List<Float> comparisons = new ArrayList<>();
        for (int i = 0; i < tools.size(); i++) {
            for (int j = i + 1; j < tools.size(); j++) {
                compare(tokenCounts.get(i), tokenCounts.get(j), comparisons);
            }
        }

        insert.clearParameters();

        int i = 0;
        insert.setString(++i, t.getId());
        for (Metadata m : metadatas) {
            Integer extractStatus = m.getInt(EXTRACT_STATUS);
            if (extractStatus == null) {
                insert.setNull(++i, Types.INTEGER);
            } else {
                insert.setInt(++i, extractStatus);
            }
            for (Property p : EVAL_PROPS) {
                if (p.equals(NUM_TOKENS) || p.equals(NUM_ALPHA_TOKENS) || p.equals(NUM_COMMON_TOKENS)) {
                    updateInteger(insert, ++i, m.getInt(p));
                } else if (p.equals(LANGUAGE)) {
                    updateString(insert, ++i, m.get(p));
                } else if (p.equals(OUT_OF_VOCABULARY)) {
                    updateDouble(insert, ++i, m.get(p));
                }
            }
        }
        for (Float comparison : comparisons) {
            updateFloat(insert, ++i, comparison);
        }
        long processed = TUPLES_PROCESSED.get();
        LOGGER.debug("about to insert " + t.getFetchKey().getFetchKey());
        insert.execute();
        LOGGER.debug("inserted " + t.getFetchKey().getFetchKey());
    }

    private void load(String tool, String fetchKey, String extension, List<Metadata> metadatas) {
        String theFetchKey = fetchKey;
        if (! StringUtils.isBlank(extension)) {
            theFetchKey += "." + extension;
        }
        if (extension.equals("json")) {
            try (InputStream is = fetcherManager.getFetcher(tool).fetch(theFetchKey,
                    new Metadata())) {
                if (TikaInputStream.isTikaInputStream(is)) {
                    TikaInputStream tis = (TikaInputStream) is;
                    if (tis.getLength() > maxInputStreamLength) {
                        Metadata m = new Metadata();
                        m.set(EXTRACT_STATUS, TOO_LONG.ordinal());
                        metadatas.add(m);
                        return;
                    }
                }
                try (Reader reader =
                         new BufferedReader(new InputStreamReader(is,
                                 StandardCharsets.UTF_8))) {
                    List<Metadata> m = JsonMetadataList.fromJson(reader);
                    m.get(0).set(EXTRACT_STATUS, OK.ordinal());
                    metadatas.add(m.get(0));
                    return;
                }
            } catch (Exception e) {
                Metadata m = new Metadata();
                m.set(EXTRACT_STATUS, MultiComparerCLI.EXTRACT_STATUS.MISSING.ordinal());
                metadatas.add(m);
                return;
            }
        } else if (extension.equals("txt")) {
            Metadata metadata = new Metadata();
            try (InputStream is = fetcherManager.getFetcher(tool).fetch(theFetchKey,
                    metadata)) {
                if (TikaInputStream.isTikaInputStream(is)) {
                    TikaInputStream tis = (TikaInputStream) is;
                    if (tis.getLength() > maxInputStreamLength) {
                        Metadata m = new Metadata();
                        m.set(EXTRACT_STATUS, TOO_LONG.ordinal());
                        return;
                    }
                }
                String txt = IOUtils.toString(is, StandardCharsets.UTF_8);
                metadata.set(TikaCoreProperties.TIKA_CONTENT, txt);
                metadata.set(EXTRACT_STATUS, OK.ordinal());
                metadatas.add(metadata);
            } catch (Exception e) {
                Metadata m = new Metadata();
                m.set(EXTRACT_STATUS, MultiComparerCLI.EXTRACT_STATUS.MISSING.ordinal());
                metadatas.add(m);
                return;
            }
        } else {
            throw new IllegalArgumentException("Sorry, I only understand 'json' or 'txt' as " +
                    "extension options");
        }
    }

    private void updateFloat(PreparedStatement insert, int i, Float f) throws SQLException {
        if (f == null) {
            insert.setNull(i, Types.FLOAT);
            return;
        }
        insert.setFloat(i, f);
    }

    private void updateDouble(PreparedStatement insert, int i, String s) throws SQLException {
        if (s == null) {
            insert.setNull(i, Types.DOUBLE);
            return;
        }
        insert.setDouble(i, Double.parseDouble(s));
    }

    private void updateString(PreparedStatement insert, int i, String s) throws SQLException {
        if (s == null) {
            insert.setNull(i, Types.VARCHAR);
            return;
        }
        insert.setString(i, s);
    }

    private void updateInteger(PreparedStatement insert, int i, Integer anInt) throws SQLException {
        if (anInt == null) {
            insert.setNull(i, Types.INTEGER);
            return;
        }
        insert.setInt(i, anInt);
    }

    private void compare(TokenCounts tokenCountsA, TokenCounts tokenCountsB,
                         List<Float> comparisons) {
        ContrastStatistics contrastStatistics =
                tokenContraster.calculateContrastStatistics(tokenCountsA, tokenCountsB);
        comparisons.add((float)contrastStatistics.getDiceCoefficient());
        comparisons.add((float)contrastStatistics.getOverlap());
    }

    private PreparedStatement prepareInsert() throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(MultiComparerCLI.TABLE_NAME);
        sb.append(" values (?");
        int toolInfos = tools.size() * 6; // five info items per tool, plus extract status
        int toolComparisons = 0;
        for (int i = 0; i < tools.size() ; i++) {
            for (int j = i+1; j < tools.size(); j++) {
                toolComparisons += 2;//overlap and dice
            }
        }
        for (int i = 0; i < toolInfos + toolComparisons; i++) {
            sb.append(",?");
        }
        sb.append(")");
        return connection.prepareStatement(sb.toString());
    }

    public TokenCounts filter(Metadata metadata) throws TikaException {
        String content = metadata.get(TikaCoreProperties.TIKA_CONTENT);
        if (StringUtils.isAllBlank(content)) {
            if (OK.ordinal() == metadata.getInt(EXTRACT_STATUS)) {
                metadata.set(EXTRACT_STATUS, MultiComparerCLI.EXTRACT_STATUS.EMPTY.ordinal());
            }
            return new TokenCounts();
        }
        return calcStats(content, metadata);
    }

    private TokenCounts calcStats(String content, Metadata metadata) {
        Map<Class, Object> results = TEXT_STATS_CALCULATOR.calculate(content);

        TokenCounts tokenCounts = (TokenCounts) results.get(BasicTokenCountStatsCalculator.class);
        metadata.set(NUM_TOKENS, tokenCounts.getTotalTokens());
        metadata.set(NUM_UNIQUE_TOKENS, tokenCounts.getTotalUniqueTokens());


        //common token results
        CommonTokenResult commonTokenResult = (CommonTokenResult) results.get(CommonTokens.class);
        metadata.set(NUM_ALPHA_TOKENS, commonTokenResult.getAlphabeticTokens());
        metadata.set(NUM_UNIQUE_ALPHA_TOKENS, commonTokenResult.getUniqueAlphabeticTokens());
        metadata.set(NUM_COMMON_TOKENS, commonTokenResult.getCommonTokens());
        if (commonTokenResult.getAlphabeticTokens() > 0) {
            metadata.set(OUT_OF_VOCABULARY, commonTokenResult.getOOV());
        } else {
            metadata.set(OUT_OF_VOCABULARY, -1.0f);
        }

        //languages
        List<LanguageResult> probabilities =
                (List<LanguageResult>) results.get(LanguageIDWrapper.class);
        if (probabilities.size() > 0) {
            metadata.set(LANGUAGE, probabilities.get(0).getLanguage());
            metadata.set(LANGUAGE_CONFIDENCE, probabilities.get(0).getRawScore());
        }
        return tokenCounts;
    }

    private static class Pair {
        private final Metadata metadata;
        private final TokenCounts tokenCounts;

        private Pair(Metadata metadata, TokenCounts tokenCounts) {
            this.metadata = metadata;
            this.tokenCounts = tokenCounts;
        }
    }
}
