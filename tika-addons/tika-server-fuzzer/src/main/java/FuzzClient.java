import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.ws.rs.core.Response;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.pdfbox.cos.COSName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.fuzzing.AutoDetectTransformer;
import org.apache.tika.fuzzing.Transformer;
import org.apache.tika.fuzzing.general.ByteDeleter;
import org.apache.tika.fuzzing.general.ByteFlipper;
import org.apache.tika.fuzzing.general.ByteInjector;
import org.apache.tika.fuzzing.general.GeneralTransformer;
import org.apache.tika.fuzzing.general.SpanSwapper;
import org.apache.tika.fuzzing.general.Truncator;
import org.apache.tika.fuzzing.pdf.PDFTransformer;
import org.apache.tika.fuzzing.pdf.PDFTransformerConfig;
import org.apache.tika.metadata.ExternalProcess;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.mime.MediaType;
import org.apache.tika.utils.StringUtils;

public class FuzzClient {

    private final static Path EXIT_SEMAPHORE = Paths.get("");
    private final static int MAX_TRIES = 2;
    private final static long TIMEOUT_MS = 300000;
    private final static long MAX_SIZE_BYTES = 1000000;
    private final static int ITERATIONS_PER_FILE = 5000;
    //we don't care about these exit values
    private Set<Integer> boringExitValues = Set.of(0);
    private final String[] tikaUrls;
    private final Path seedDir;
    private final Path outputDir;
    private final int iterationsPerFile;
    Logger LOG = LoggerFactory.getLogger(FuzzClient.class);

    public FuzzClient(String[] tikaUrls, Path seedDir, Path outputDir, int iterationsPerFile) {
        this.tikaUrls = tikaUrls;
        this.seedDir = seedDir;
        this.outputDir = outputDir;
        this.iterationsPerFile = iterationsPerFile;
    }

    public static void main(String[] args) throws Exception {
        String[] tikaUrls = new String[]{
                "http://localhost:9990",
                "http://localhost:9991",
                "http://localhost:9992",
                "http://localhost:9993",
                "http://localhost:9994",
                "http://localhost:9995",
                "http://localhost:9996",
                "http://localhost:9997",
                "http://localhost:9998",
                "http://localhost:9999"

        };
        Path seedDir = Paths.get("/Users/allison/data/tmp");
        Path outputDir = Paths.get("/Users/allison/data/onenote-fuzzed");
        int iterationsPerFile = ITERATIONS_PER_FILE;
        FuzzClient fuzzClient = new FuzzClient(tikaUrls, seedDir, outputDir, iterationsPerFile);
        fuzzClient.execute();
    }

    private void execute() throws Exception {
        int numThreads = tikaUrls.length;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads+1);
        ExecutorCompletionService executorCompletionService =
                new ExecutorCompletionService(executorService);
        ArrayBlockingQueue<Path> queue = new ArrayBlockingQueue<>(1000);
        executorCompletionService.submit(new FileEnqueuer(queue));

        for (int i = 0; i < tikaUrls.length; i++) {
            executorCompletionService.submit(new Fuzzer(tikaUrls[i], queue));
        }
        int finished = 0;
        try {
            while (finished < numThreads + 1) {
                //blocks
                Future<Integer> future = executorCompletionService.take();
                future.get();
                finished++;
            }
        } finally {
            executorService.shutdownNow();
        }

    }
    private class FileEnqueuer implements Callable<Integer> {
        private final ArrayBlockingQueue<Path> queue;

        public FileEnqueuer(ArrayBlockingQueue<Path> queue) {
            this.queue = queue;
        }

        @Override
        public Integer call() throws Exception {
            Files.walkFileTree(seedDir, new MyFileVisitor(queue));
            queue.put(EXIT_SEMAPHORE);
            return 1;
        }
    }

    private class MyFileVisitor implements FileVisitor<Path> {

        private final ArrayBlockingQueue<Path> queue;

        public MyFileVisitor(ArrayBlockingQueue<Path> queue) {
            this.queue = queue;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                throws IOException {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            if (MAX_SIZE_BYTES < 0 || Files.size(file) < MAX_SIZE_BYTES) {
                try {
                    queue.put(file);
                    LOG.info("enqueued: " + file);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
            return FileVisitResult.CONTINUE;
        }


        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
        }
    }

    private class Fuzzer implements Callable<Integer> {

        Transformer transformer = //new NullTransformer();//PDFTransformer();
                new GeneralTransformer(1, new ByteDeleter(), new ByteFlipper(),
                new ByteInjector(), new Truncator(), new SpanSwapper());

        private final String tikaUrl;
        private final ArrayBlockingQueue<Path> queue;

        public Fuzzer(String tikaUrl, ArrayBlockingQueue<Path> queue) {
            PDFTransformerConfig c = new PDFTransformerConfig();
            c.setMaxFilters(10000);
            c.setMinFilters(1);
            c.setRandomizeRefNumbers(0.01f);
            c.setRandomizeObjectNumbers(0.01f);
            Set<COSName> allowableFilters = new HashSet<>();
            allowableFilters.add(COSName.FLATE_DECODE);
            allowableFilters.add(COSName.ASCII85_DECODE);
            allowableFilters.add(COSName.ASCII_HEX_DECODE);
            allowableFilters.add(COSName.ASCII85_DECODE_ABBREVIATION);
            allowableFilters.add(COSName.ASCII_HEX_DECODE_ABBREVIATION);
            c.setAllowableFilters(allowableFilters);
            //((PDFTransformer)transformer).setConfig(c);
            this.tikaUrl = tikaUrl;
            this.queue = queue;
        }

        protected void fuzz(Path file) throws InterruptedException, IOException {
            Path tmp = Files.createTempFile("fuzz-", "");
            LOG.info("processing " + file);
            try {
                fuzzIt(file, tmp);
            } finally {
                try {
                    Files.delete(tmp);
                } catch (IOException e) {
                    LOG.warn("couldn't delete "+ tmp, e);
                }
            }
        }

        protected void fuzzIt(Path seed, Path output) throws InterruptedException,
                IOException {
            String url = tikaUrl + "/rmeta/text";

            int returnVal = tryOne(url, seed);
            if (returnVal != 200) {
                return;
            }
            try (InputStream is = Files.newInputStream(seed);
                 OutputStream os = Files.newOutputStream(output)) {
                transformer.transform(is, os);
            } catch (StackOverflowError|IllegalStateException|IOException e) {
                //pdfbox couldn't load the file?
                //pdfbox can't write encryption dictionary
                return;
            } catch (TikaException e) {
                e.printStackTrace();
            }
            if (Files.size(output) == 0) {
                return;
            }
            Response response = null;
            int tries = 0;
            while (response == null && tries++ < MAX_TRIES) {
                try (InputStream is = Files.newInputStream(output)) {
                    WebClient webClient = WebClient.create(url);
                    HTTPConduit conduit = WebClient.getConfig(webClient).getHttpConduit();
                    conduit.getClient().setConnectionTimeout(TIMEOUT_MS);
                    conduit.getClient().setReceiveTimeout(TIMEOUT_MS);
                    conduit.getClient().setConnectionRequestTimeout(TIMEOUT_MS);
                    response = webClient.accept("application/json").put(is);
                } catch (Exception e) {
                    LOG.warn("problem with server {}", e);
                    Thread.sleep(10000);
                }
            }
            if (response == null) {
                reportCrash("server crash", "null", null, seed, output);
                return;
            }

            if (response.getStatus() == 200) {
                Reader reader = new InputStreamReader((InputStream) response.getEntity(), UTF_8);
                List<Metadata> metadataList = JsonMetadataList.fromJson(reader);
                if (metadataList != null && metadataList.size() > 0) {
                    Metadata m = metadataList.get(0);
                    String[] parsedBy = m.getValues(TikaCoreProperties.TIKA_PARSED_BY);
                    if (parsedBy == null || parsedBy.length == 0) {
                        LOG.warn("no parsed by");
                        return;
                    }
                    if (m.get(TikaCoreProperties.CONTAINER_EXCEPTION) != null) {
                        System.out.println("runtime :" + seed.getFileName() + "; " +
                                        m.get(TikaCoreProperties.CONTAINER_EXCEPTION));
                    }
                    if (!StringUtils.isBlank(m.get(ExternalProcess.STD_ERR))) {
                        System.out.println("stderr: " + m.get(ExternalProcess.STD_ERR));
                    }

                    String mime = m.get(Metadata.CONTENT_TYPE);
                    if (! parsedBy[parsedBy.length-1].equals("org.apache.tika.parser.external2.ExternalParser")) {
                        for (String b : parsedBy) {
                            System.out.println("parsed by " + b + " " + mime);
                        }
                        return;
                    }
                    String exitString = m.get(ExternalProcess.EXIT_VALUE);
                    if (exitString != null) {
                        //System.out.println(seed + " " + iteration + " " + exitString);
                        int exitValue = Integer.parseInt(exitString);
                        if (! boringExitValues.contains(exitValue)) {
                            reportCrash("exitValue: " + exitValue, exitString,
                                    metadataList, seed, output);
                        }
                    } else {
                        LOG.warn("NO exit string?!");
                    }
                } else {
                    LOG.warn("empty metadata list {}", seed);
                }
            } else {
                LOG.warn("bad status {}", seed);
            }
        }

        private int tryOne(String url, Path seed) {
            try (InputStream is = Files.newInputStream(seed)) {
                WebClient webClient = WebClient.create(url);
                HTTPConduit conduit = WebClient.getConfig(webClient).getHttpConduit();
                conduit.getClient().setConnectionTimeout(TIMEOUT_MS);
                conduit.getClient().setReceiveTimeout(TIMEOUT_MS);
                conduit.getClient().setConnectionRequestTimeout(TIMEOUT_MS);
                Response response = webClient.accept("application/json").put(is);
                return response.getStatus();
            } catch (Exception e) {
                LOG.warn("problem with server {}", e);
            }
            return -1;
        }

        private void reportCrash(String msg, String exitValue, List<Metadata> metadataList,
                                 Path seed,
                                 Path fuzzed) {
            try {
                String seedDigest = "";
                try (InputStream is = Files.newInputStream(seed)) {
                    seedDigest = DigestUtils.sha256Hex(is);
                }
                String fuzzedDigest = "";
                try (InputStream is = Files.newInputStream(fuzzed)) {
                    fuzzedDigest = DigestUtils.sha256Hex(is);
                }
                if (exitValue.startsWith("-")) {
                    exitValue = "neg-" + exitValue.substring(1);
                }
                String nameBase = exitValue+"/" + seed.getFileName().toString() + "-"+fuzzedDigest;
                String name = nameBase+".pdf";
                LOG.error("crash: {} -> {}: {}", tikaUrl, exitValue, name);

                Path target = outputDir.resolve("files").resolve(name);
                if (!Files.isDirectory(target.getParent())) {
                    Files.createDirectories(target.getParent());
                }
                if (!Files.isRegularFile(target)) {
                    try {
                        Files.copy(fuzzed, target);
                    } catch (IOException e) {
                        LOG.warn("couldn't copy file: ", e);
                    }
                }
                if (metadataList != null && metadataList.size() > 0) {
                    Path extract = outputDir.resolve("extracts").resolve(nameBase+".json");
                    if (! Files.isDirectory(extract.getParent())) {
                        Files.createDirectories(extract.getParent());
                    }
                    try (Writer writer = Files.newBufferedWriter(extract, UTF_8)) {
                        JsonMetadataList.toJson(metadataList, writer);
                    }
                }
            } catch (IOException e) {
                LOG.warn("surprise io exception writing reporting file", e);
            }
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                //blocking
                Path p = queue.take();
                if (p == EXIT_SEMAPHORE) {
                    LOG.info("received exit semaphore; I'm done");
                    queue.put(EXIT_SEMAPHORE);
                    return 1;
                }
                LOG.info("fuzzing "+p.getFileName());
                for (int i = 0; i < iterationsPerFile; i++) {
                    fuzz(p);
                }
            }
        }
    }

    private static class NullTransformer implements Transformer {

        @Override
        public Set<MediaType> getSupportedTypes() {
            return null;
        }

        @Override
        public void transform(InputStream inputStream, OutputStream outputStream)
                throws IOException, TikaException {
            IOUtils.copy(inputStream, outputStream);
        }
    }
}
