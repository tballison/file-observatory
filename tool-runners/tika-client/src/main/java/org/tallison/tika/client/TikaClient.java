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

public class TikaClient {
    private static final int TIMEOUT_SECONDS = 120;

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
                    boolean success = extract(webClient, tikaUrl, executorService, in, out);
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

    public static HttpClient getNewClient() {
        RequestConfig config = RequestConfig.custom().setConnectTimeout(TIMEOUT_SECONDS * 1000)
                .setConnectionRequestTimeout(TIMEOUT_SECONDS * 1000)
                .setSocketTimeout(TIMEOUT_SECONDS * 1000).build();
        return HttpClients.custom().setDefaultRequestConfig(config).build();
    }

    public static boolean extract(HttpClient webClient, String tikaUrl,
                                   ExecutorService executorService, Path in, Path out)
            throws IOException {
        if (!Files.exists(in)) {
            System.err.println("File does not exist: " + in.toAbsolutePath());
            return true;
        }
        if (Files.exists(out)) {
            System.err.println("Output file already exists: " + out.toAbsolutePath());
            return true;
        }
        Files.createDirectories(out.getParent());
        Files.write(out, new byte[]{});
        FutureTask<Integer> futureTask = new FutureTask<>(() -> {
            long start = System.currentTimeMillis();
            HttpPut put = new HttpPut(tikaUrl);
            put.setEntity(new FileEntity(in.toFile()));
            HttpResponse response = null;
            try {
                response = webClient.execute(put);
                long elapsed = System.currentTimeMillis() - start;
                System.out.println(in.toAbsolutePath() + " : " + elapsed + " " +
                        response.getStatusLine().getStatusCode());
                if (response.getStatusLine().getStatusCode() == 200) {
                    Files.copy(response.getEntity().getContent(), out, StandardCopyOption.REPLACE_EXISTING);
                } else {
                    System.out.println("bad result " + response.getStatusLine().getStatusCode());
                }
            } finally {
                if (response != null && (response instanceof CloseableHttpResponse)) {
                    ((CloseableHttpResponse)response).close();
                }
            }

            return 1;
        });
        executorService.submit(futureTask);
        try {
            futureTask.get(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        } catch (ExecutionException e) {
            e.printStackTrace();
            return false;
        } catch (TimeoutException e) {
            System.err.println("timeout: " + in.toAbsolutePath() + " " + Files.size(in));
            return false;
        } finally {
            futureTask.cancel(true);
        }
        return true;
    }
}
