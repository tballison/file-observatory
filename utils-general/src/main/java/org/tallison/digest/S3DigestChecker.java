package org.tallison.digest;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.codec.digest.DigestUtils;

import org.apache.tika.utils.StringUtils;

public class S3DigestChecker {
    static AtomicInteger COUNTER = new AtomicInteger(0);
    public static void main(String[] args) throws Exception {
        Path outputPath = Paths.get("");
        String bucket = "";
        String prefix = "";//(args.length > 1) ? args[2] : null;
        System.out.println("writer: "+ outputPath);
        System.out.println("bucket : " + bucket);
        System.out.println("prefix : " + prefix);
        AWSCredentialsProvider provider = DefaultAWSCredentialsProviderChain.getInstance();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion("us-east-1").withCredentials(provider).build();

        S3DigestChecker checker = new S3DigestChecker();
        try (BufferedWriter writer = Files.newBufferedWriter(
                outputPath, StandardCharsets.UTF_8);
        ) {
            checker.execute(s3Client, bucket, prefix, writer);
        }
        System.out.println("finished successfully");

    }

    private void execute(AmazonS3 s3Client, String bucket, String prefix,
                         BufferedWriter writer) throws Exception {
        ArrayBlockingQueue<String> paths = new ArrayBlockingQueue<>(10000);
        ArrayBlockingQueue<FileDigestPair> pairs = new ArrayBlockingQueue<>(1000);
        int numWorkers = 50;

        ExecutorService executorService = Executors.newFixedThreadPool(numWorkers+1);
        ExecutorCompletionService<Integer> executorCompletionService =
                new ExecutorCompletionService<>(executorService);
        for (int i = 0; i < numWorkers; i++) {
            executorCompletionService.submit(new DigestWorker(s3Client, bucket, paths, pairs));
        }
        executorCompletionService.submit(new DiffWriter(pairs, writer));
        int cnt = 0;
        for (S3ObjectSummary summary : S3Objects.withPrefix(s3Client, bucket, prefix)) {
            String k = summary.getKey();
            if (! StringUtils.isBlank(k)) {
                paths.put(k);
            }
            cnt++;
            if (cnt % 1000 == 0) {
                System.out.println("added " + cnt);
            }
        }

        for (int i = 0; i < numWorkers; i++) {
            paths.put("");
        }

        int finished = 0;
        while (finished < numWorkers) {
            Future<Integer> future = executorCompletionService.take();
            future.get();
            finished++;
        }
        pairs.put(new FileDigestPair(null, null));

        Future<Integer> writerFuture = executorCompletionService.take();
        writerFuture.get();
        executorService.shutdownNow();
    }

    private static class DiffWriter implements Callable<Integer> {
        private final ArrayBlockingQueue<FileDigestPair> pairs;
        private final BufferedWriter writer;

        DiffWriter(ArrayBlockingQueue<FileDigestPair> pairs, BufferedWriter writer) {
            this.pairs = pairs;
            this.writer = writer;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                FileDigestPair p = pairs.take();
                if (p.digest == null) {
                    return 1;
                }
                System.out.println("writing bad "+p.file + " " + p.digest);
                writer.write(p.file+"\t"+p.digest+"\n");
            }
        }
    }

    private static class DigestWorker implements
            Callable<Integer> {
        private final AmazonS3 s3Client;
        private final String bucket;
        private final ArrayBlockingQueue<String> paths;
        private final ArrayBlockingQueue<FileDigestPair> fileDigestPairs;

        DigestWorker(AmazonS3 s3Client, String bucket, ArrayBlockingQueue<String> paths,
                     ArrayBlockingQueue<FileDigestPair> fileDigestPairs) {
            this.s3Client = s3Client;
            this.bucket = bucket;
            this.paths = paths;
            this.fileDigestPairs = fileDigestPairs;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                String path = paths.take();
                if (StringUtils.isBlank(path)) {
                    return 1;
                } else {
                    processPath(path);
                }
            }
        }

        private void processPath(String path) throws InterruptedException {
            int cnt = COUNTER.getAndIncrement();
            if (cnt % 1000 == 0) {
                System.out.println("processed " + cnt + " files");
            }
            Path tmp = null;
            try {
                tmp = Files.createTempFile("s3digest-", "");
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            try {
                try (S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, path))) {
                    Files.copy(s3Object.getObjectContent(), tmp, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                int i = path.lastIndexOf("/");
                String fName = path.substring(i + 1);
                String digest = null;
                try (InputStream is = Files.newInputStream(tmp)) {
                    digest = DigestUtils.sha256Hex(is);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
                if (!fName.equals(digest)) {
                    System.out.println("bad: " + fName + " " + digest);
                    fileDigestPairs.put(new FileDigestPair(fName, digest));
                }
            } finally {
                try {
                    Files.delete(tmp);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static class FileDigestPair {
        private final String file;
        private final String digest;

        public FileDigestPair(String file, String digest) {
            this.file = file;
            this.digest = digest;
        }
    }
}
