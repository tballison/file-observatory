package org.tallison.tika.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;

public class TikaLoadTester {
    enum STATUS {
        INPUT_FILE_DOESNT_EXIST,
        OK,
        TIMEOUT,
        NOT_OK
    }
    private static final long SLEEP_ON_ERROR_MS = 30000;
    private static final int TIMEOUT_SECONDS = 120;
    private static final int NUM_THREADS = 50;
    private static final int TESTS_PER_FILE = 50;
    //there should be a key in both metadata objects for this field
    //but it is ok if the values for these fields differ
    private static Set<String> IGNORE_CONTENTS = new HashSet<>(Arrays.asList(
            new String[]{
                    "X-TIKA:EXCEPTION:runtime", //tika-server can return different stack traces
                    "X-TIKA:VIOLATION",
                    "X-TIKA:VIOLATION:runtime",
                    "X-TIKA:parse_time_millis",
                    //parsely
                    "safedocs:last_line",

            }
    )
    );

    public static void main(String[] args) throws Exception {
        Path baseDir = Paths.get(args[0]);
        Path fileList = Paths.get(args[1]);
        String tikaUrl = args[2];
        HttpClient webClient = getNewClient();
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        try (BufferedReader r = Files.newBufferedReader(fileList, StandardCharsets.UTF_8)) {
            String line = r.readLine();
            while (line != null) {
                Path in = baseDir.resolve(line);
                try {
                    boolean success = testOne(webClient, tikaUrl, executorService, in);
                    if (!success) {
                        if (webClient instanceof CloseableHttpClient) {
                            ((CloseableHttpClient) webClient).close();
                        }
                        webClient = getNewClient();
                    }
                } catch (Exception e) {
                    System.err.println(in);
                    e.printStackTrace();
                }
                line = r.readLine();
            }
        } finally {
            executorService.shutdownNow();
        }
    }

    private static boolean testOne(HttpClient webClient, String tikaUrl,
                                   ExecutorService executorService, Path in) throws Exception {
        ServerResult result = null;
        try {
            result = extract("truth", webClient, tikaUrl, executorService, in, 3);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        if (result.getStatus() != STATUS.OK ||
                result.getMetadataList().size() == 0) {
            System.err.println("no ground truth: " + in.toAbsolutePath()
                    + ": " + result);
            return false;
        }
        ExecutorService myExecutorService = Executors.newFixedThreadPool(NUM_THREADS);
        ExecutorCompletionService<Integer> completionService =
                new ExecutorCompletionService<>(myExecutorService);

        for (int i = 0; i < NUM_THREADS; i++) {
            completionService.submit(new TikaTester(getNewClient(), in, tikaUrl,
                    result.getMetadataList()));
        }
        int finished = 0;
        try {
            while (finished < NUM_THREADS) {
                //block
                Future<Integer> future = completionService.take();
                future.get();
                finished++;
            }
        } finally {
            myExecutorService.shutdownNow();
        }
        return true;
    }

    private static class TikaTester implements Callable<Integer> {
        private static AtomicInteger COUNTER = new AtomicInteger(0);
        private final int id = COUNTER.getAndIncrement();
        private final HttpClient client;
        private final Path input;
        private final String tikaUrl;
        private final ExecutorService executorService = Executors.newFixedThreadPool(1);
        private final List<Metadata> expected;
        public TikaTester(HttpClient client, Path input, String tikaUrl, List<Metadata> expected) {
            this.client = client;
            this.input = input;
            this.tikaUrl = tikaUrl;
            this.expected = expected;
        }

        @Override
        public Integer call() throws Exception {
            for (int i = 0; i < TESTS_PER_FILE; i++) {
                ServerResult result = extract(
                        Integer.toString(id), client, tikaUrl, executorService, input);
                if (result.getStatus() != STATUS.OK
                        || result.getMetadataList().size() == 0) {
                    System.err.println("bad result " + result);
                    continue;
                }
                if (! equals(expected, result.getMetadataList())) {
                    System.err.println("failure: " + input.toAbsolutePath());
                }
            }
            return 1;
        }

        private boolean equals(List<Metadata> expected, List<Metadata> metadataList) {
            if (expected.size() != metadataList.size()) {
                System.out.println("diff list sizes: " + expected.size() + "->"+
                        metadataList.size());
                return false;
            }
            for (int i = 0; i < expected.size(); i++) {
                for (String n : expected.get(i).names()) {
                    String[] expectedVals = expected.get(i).getValues(n);
                    String[] vals = metadataList.get(i).getValues(n);
                    if (expectedVals.length != vals.length) {
                        System.out.println(i + " : " + n + " :arr length: " + expectedVals.length +
                                " -> " + vals.length);
                        return false;
                    }
                    if (IGNORE_CONTENTS.contains(n)) {
                        continue;
                    }
                    for (int j = 0; j < expectedVals.length; j++) {
                        if (! expectedVals[j].equals(vals[j])) {
                            System.out.println(i + " : " + n +  " :content: " + expectedVals[j] +
                                    " -> " + vals[j]);
                            return false;
                        }
                    }
                }
            }
            return true;
        }
    }

    private static HttpClient getNewClient() {
        RequestConfig config = RequestConfig.custom().setConnectTimeout(TIMEOUT_SECONDS * 1000)
                .setConnectionRequestTimeout(TIMEOUT_SECONDS * 1000)
                .setSocketTimeout(TIMEOUT_SECONDS * 1000).build();
        return HttpClients.custom().setDefaultRequestConfig(config).build();
    }

    public static ServerResult extract(String id, HttpClient webClient, String tikaUrl,
                                       ExecutorService executorService, Path in, int retries)
            throws InterruptedException {

        ServerResult result = null;
        int tries = 0;
        while (tries < retries) {
            try {
                result = extract(id, webClient, tikaUrl, executorService, in);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (result.getStatus() == STATUS.NOT_OK) {
                System.out.println("bad status, going to sleep for try: " + tries);
                Thread.sleep(SLEEP_ON_ERROR_MS);
            } else {
                return result;
            }
            tries++;
        }
        return new ServerResult(STATUS.NOT_OK, null);
    }

    public static ServerResult extract(String id, HttpClient webClient, String tikaUrl,
                                          ExecutorService executorService, Path in)
            throws IOException {
        if (!Files.exists(in)) {
            System.err.println("File does not exist: " + in.toAbsolutePath());
            return new ServerResult(STATUS.INPUT_FILE_DOESNT_EXIST, null);
        }
        FutureTask<ServerResult> futureTask = new FutureTask<>(() -> {
            long start = System.currentTimeMillis();
            HttpPut put = new HttpPut(tikaUrl);
            put.setEntity(new FileEntity(in.toFile()));
            HttpResponse response = null;
            try {
                response = webClient.execute(put);
                long elapsed = System.currentTimeMillis() - start;
                if (id.equals("truth")) {
                    System.out.println(
                            "thread-" + id + " " + in.toAbsolutePath() + " : " + elapsed + " " +
                                    response.getStatusLine().getStatusCode());
                }
                if (response.getStatusLine().getStatusCode() == 200) {
                    try (BufferedReader reader =
                                 new BufferedReader(new InputStreamReader(
                                         response.getEntity().getContent(),
                                         StandardCharsets.UTF_8))) {
                        List<Metadata> metadataList = JsonMetadataList.fromJson(reader);
                        if (metadataList.size() == 0) {
                            System.out.println("empty list: " + in.toAbsolutePath());
                            return new ServerResult(STATUS.NOT_OK, metadataList);
                        } else {
                            return new ServerResult(STATUS.OK, metadataList);
                        }
                    }
                } else {
                    System.out.println("bad result " + response.getStatusLine().getStatusCode());
                }
            } finally {
                if (response != null && (response instanceof CloseableHttpResponse)) {
                    ((CloseableHttpResponse)response).close();
                }
            }

            return new ServerResult(STATUS.NOT_OK, null);
        });
            executorService.submit(futureTask);
        try {
            return futureTask.get(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return new ServerResult(STATUS.NOT_OK, null);
        } catch (ExecutionException e) {
            e.printStackTrace();
            return new ServerResult(STATUS.NOT_OK, null);
        } catch (TimeoutException e) {
            System.err.println("timeout: " + in.toAbsolutePath() + " " + Files.size(in));
            return new ServerResult(STATUS.TIMEOUT, null);
        } finally {
            futureTask.cancel(true);
        }
    }

    public static class ServerResult {
        private final STATUS status;
        private final List<Metadata> metadataList;

        public STATUS getStatus() {
            return status;
        }

        public List<Metadata> getMetadataList() {
            return metadataList;
        }

        public ServerResult(STATUS status, List<Metadata> metadataList) {
            this.status = status;
            this.metadataList = metadataList;
        }

        @Override
        public String toString() {
            return "ServerResult{" + "status=" + status + ", metadataList=" + metadataList + '}';
        }
    }
}
