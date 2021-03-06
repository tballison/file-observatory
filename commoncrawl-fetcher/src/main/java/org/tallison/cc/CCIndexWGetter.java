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
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * This uses 'wget' to grab the index files from a specific
 * common crawl collection, e.g. CC-MAIN-2020-45.
 *
 * To get the latest crawl, see: https://commoncrawl.org/connect/blog/
 *
 * If you're already on AWS and have access to s3, do the processing there!
 */
public class CCIndexWGetter {
    Logger LOGGER = LoggerFactory.getLogger(CCIndexWGetter.class);
    private static final int POISON = -1;
    private static String AWS_URL_BASE = "https://commoncrawl.s3.amazonaws.com/cc-index/collections/";
    private static String AWS_URL_INDICES = "/indexes/cdx-";

    private static final int DEFAULT_NUM_THREADS = 3;//be nice...don't do more than this

    private static Options getOptions() {
        Options options = new Options();

        options.addRequiredOption("i", "index", true,
                "index to grab, e.g. CC-MAIN-2020-10");
        options.addRequiredOption("o", "outputDir", true,
                "directory to which to write the index files");
        options.addOption("m", "max", true,
                "maximum number of index files to fetch");
        options.addOption("n", "numThreads", true,
                "number of threads.  Don't use more than 3!");

        return options;
    }


    public static void main(String[] args) throws Exception {
        CommandLineParser cliParser = new DefaultParser();
        CommandLine line = cliParser.parse(getOptions(), args);


        String index = line.getOptionValue("i");
        Path outDir = Paths.get(line.getOptionValue("o"));

        int numThreads = DEFAULT_NUM_THREADS;
        if (line.hasOption("n")) {
            numThreads = Integer.parseInt(line.getOptionValue("n"));
        }
        int max = 300;
        if (line.hasOption("m")) {
            max = Integer.parseInt(line.getOptionValue("m"));
        }
        CCIndexWGetter wgetter = new CCIndexWGetter();
        wgetter.execute(index, outDir, numThreads, max);
    }

    private void execute(String collection, Path outDir, int numThreads, int max) throws IOException {
        System.out.println("going to get "+collection +" with " + numThreads + " threads");
        ExecutorService es = Executors.newFixedThreadPool(numThreads);
        ExecutorCompletionService<Integer> completionService = new ExecutorCompletionService<>(es);
        ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(max+numThreads);

        for (int i = 0; i < max; i++) {
            queue.add(i);
        }
        for (int i = 0; i < numThreads; i++) {
            queue.add(POISON);
        }

        if (!Files.isDirectory(outDir)) {
            Files.createDirectories(outDir);
        }
        for (int i = 0; i < numThreads; i++) {
            completionService.submit(new Getter(collection, outDir, queue));
        }

        int completed = 0;
        while (completed < numThreads) {
            Future<Integer> fut = null;
            try {
                fut = completionService.take();
                fut.get();
                completed++;
            } catch (InterruptedException|ExecutionException e) {
                e.printStackTrace();
                break;
            }
        }
        es.shutdownNow();

    }

    private class Getter implements Callable<Integer> {

        private final String collection;
        private final Path outputDir;
        private final ArrayBlockingQueue<Integer> queue;


        Getter(String collection, Path outputDir, ArrayBlockingQueue<Integer> queue) {
            this.collection = collection;
            this.outputDir = outputDir;
            this.queue = queue;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                int indexNum = queue.take();
                if (indexNum == POISON) {
                    return 1;
                }
                String num = StringUtils.leftPad(Integer.toString(indexNum), 5, "0");
                String url = AWS_URL_BASE + collection + AWS_URL_INDICES + num + ".gz";
                LOGGER.info("about to get ({})", url);
                Path output = outputDir.resolve(collection+"-cdx-"+num+".gz");
                int tries = 0;
                boolean finished = false;
                while (! finished && ++tries < 3) {
                    ProcessBuilder pb = new ProcessBuilder("wget", "-q",
                            "-O", output.toAbsolutePath().toString(), url);
                    Process process = pb.inheritIO().start();
                    finished = process.waitFor(5, TimeUnit.MINUTES);
                    if (finished) {
                        LOGGER.info("successfully retrieved:" +output.getFileName());
                    } else {
                        LOGGER.warn("timeout on: "+output.getFileName());
                        process.destroy();
                        process.destroyForcibly();
                        //sleep a bit and hope the process has fully finished
                        //otherwise, it might not let go of the file
                        Thread.sleep(1000);
                        Files.delete(output);
                    }
                }
            }
        }
    }
}
