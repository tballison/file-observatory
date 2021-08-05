package org.tallison.tika.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.batchlite.AbstractDirectoryProcessor;

public class TikaClient {

    private static Logger LOGGER = LoggerFactory.getLogger(TikaClient.class);

    private static final int TIMEOUT_SECONDS = 120;
    private static final int SLEEP_ON_FAILURE_SECONDS = 30;
    private static final int MAX_RETRIES = 3;

    public static void main(String[] args) throws Exception {
        Path baseDir = Paths.get(args[0]);
        Path fileList = Paths.get(args[1]);
        String tikaUrl = args[2];
        Path targDir = Paths.get(args[3]);
        HttpClient webClient = getNewClient();
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        try (BufferedReader r = Files.newBufferedReader(fileList, StandardCharsets.UTF_8)) {
            String line = r.readLine();
            while (line != null) {
                Path in = baseDir.resolve(line);
                Path out = targDir.resolve(line + ".json");
                try {
                    boolean shouldProcess = shouldProcess(in, out);
                    if (! shouldProcess) {
                        line = r.readLine();
                        continue;
                    }
                    boolean success =
                            extract(webClient, tikaUrl, executorService, in, out, MAX_RETRIES);
                    if (!success) {
                        if (webClient instanceof CloseableHttpClient) {
                            ((CloseableHttpClient) webClient).close();
                        }
                        webClient = getNewClient();
                    }
                } catch (Exception e) {
                    LOGGER.error("catastrophe: {}",
                            in.toAbsolutePath(), e);
                }
                line = r.readLine();
            }
        } finally {
            executorService.shutdownNow();
        }
    }

    private static boolean shouldProcess(Path in, Path out) {
        if (!Files.exists(in)) {
            LOGGER.debug("Input file does not exist: {}", in.toAbsolutePath());
            return false;
        }
        if (Files.exists(out)) {
            LOGGER.debug("Output file already exists: {}", out.toAbsolutePath());
            return false;
        }
        return true;
    }

    public static HttpClient getNewClient() {
        RequestConfig config = RequestConfig.custom().setConnectTimeout(TIMEOUT_SECONDS * 1000)
                .setConnectionRequestTimeout(TIMEOUT_SECONDS * 1000)
                .setSocketTimeout(TIMEOUT_SECONDS * 1000).build();
        return HttpClients.custom().setDefaultRequestConfig(config).build();
    }

    public static boolean extract(HttpClient webClient, String tikaUrl,
                                  ExecutorService executorService, Path in, Path out, int retries) {

        int tries = 0;
        //retry on IOException, not timeout or other failure
        long overallStart = System.currentTimeMillis();
        while (tries++ < retries) {
            try {
                long tryStart = System.currentTimeMillis();
                boolean success = extract(webClient, tikaUrl, executorService, in, out);
                if (success) {
                    long now = System.currentTimeMillis();
                    LOGGER.info("success: {} retry={} tryElapsedMs={} totalElapsed={}",
                            in.toAbsolutePath(), tries,
                            now-tryStart,
                            now-overallStart);
                    return success;
                }
            } catch (IOException e) {
                LOGGER.warn("IO: {}", in.toAbsolutePath());
                LOGGER.debug("starting sleep on retry {}", tries);
                try {
                    Thread.sleep(SLEEP_ON_FAILURE_SECONDS * 1000);
                } catch (InterruptedException ex) {
                    LOGGER.warn("interrupted");
                    break;
                }
                LOGGER.debug("waking up after retry {}", tries);
            } catch (InterruptedException e) {
                LOGGER.warn("interrupted: {}", in.toAbsolutePath());
                break;
            } catch (ExecutionException e) {
                LOGGER.warn("failed: {}", in.toAbsolutePath(), e);
                break;
            } catch (TimeoutException e) {
                LOGGER.warn("timeout: {}", in.toAbsolutePath());
                break;
            } catch (Exception e) {
                LOGGER.warn("unknown: {}", in.toAbsolutePath(), e);
                break;
            }
        }
        long now = System.currentTimeMillis();

        LOGGER.warn("fail: {}, tries={} overallElapsed={}",
                in.toAbsolutePath(), tries, now-overallStart);
        return false;
    }


    public static boolean extract(HttpClient webClient, String tikaUrl,
                                   ExecutorService executorService, Path in, Path out)
            throws Exception {

        Files.createDirectories(out.getParent());
        Files.write(out, new byte[]{});
        FutureTask<Boolean> futureTask = new FutureTask<>(() -> {
            long start = System.currentTimeMillis();
            HttpPut put = new HttpPut(tikaUrl);
            put.setEntity(new FileEntity(in.toFile()));
            HttpResponse response = null;
            try {
                response = webClient.execute(put);
                long elapsed = System.currentTimeMillis() - start;
                if (response.getStatusLine().getStatusCode() == 200) {
                    Files.copy(response.getEntity().getContent(), out, StandardCopyOption.REPLACE_EXISTING);
                    LOGGER.debug("local success: {} elapsed={} ms status={}",
                            in.toAbsolutePath(), elapsed,
                            response.getStatusLine().getStatusCode());
                    return true;
                } else {
                    LOGGER.debug("local fail: {} {}",
                            in.toAbsolutePath(),
                            response.getStatusLine().getStatusCode());
                    return false;
                }
            } finally {
                if (response != null && (response instanceof CloseableHttpResponse)) {
                    ((CloseableHttpResponse)response).close();
                }
            }
        });

        executorService.submit(futureTask);
        try {
            return futureTask.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            //unwrap IOException
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        } finally {
            futureTask.cancel(true);
        }
    }
}
