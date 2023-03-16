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
package org.tallison.cc.fetcherlite;

import java.nio.file.Path;

public class FetcherLiteConfig {

    public static String CC_URL_BASE = "https://data.commoncrawl.org/";
    private int numThreads;
    //maximum records to read
    private long maxRecords;

    //maximum files extracted from cc
    private long maxFilesExtracted;
    //maximum files written to 'truncated' list.
    private long maxFilesTruncated;

    private Path indexPathsFile;

    private Path filesDirectory;

    private Path truncatedUrlsFile;

    private Path filterFile;

    public static String getCcUrlBase() {
        return CC_URL_BASE;
    }

    public static void setCcUrlBase(String ccUrlBase) {
        CC_URL_BASE = ccUrlBase;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public void setNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }

    public long getMaxRecords() {
        return maxRecords;
    }

    public void setMaxRecords(long maxRecords) {
        this.maxRecords = maxRecords;
    }

    public long getMaxFilesExtracted() {
        return maxFilesExtracted;
    }

    public void setMaxFilesExtracted(long maxFilesExtracted) {
        this.maxFilesExtracted = maxFilesExtracted;
    }

    public long getMaxFilesTruncated() {
        return maxFilesTruncated;
    }

    public void setMaxFilesTruncated(long maxFilesTruncated) {
        this.maxFilesTruncated = maxFilesTruncated;
    }

    public Path getIndexPathsFile() {
        return indexPathsFile;
    }

    public void setIndexPathsFile(Path indexPathsFile) {
        this.indexPathsFile = indexPathsFile;
    }

    public Path getFilesDirectory() {
        return filesDirectory;
    }

    public void setFilesDirectory(Path filesDirectory) {
        this.filesDirectory = filesDirectory;
    }

    public Path getTruncatedUrlsFile() {
        return truncatedUrlsFile;
    }

    public void setTruncatedUrlsFile(Path truncatedUrlsFile) {
        this.truncatedUrlsFile = truncatedUrlsFile;
    }

    public Path getFilterFile() {
        return filterFile;
    }

    public void setFilterFile(Path filterFile) {
        this.filterFile = filterFile;
    }
}
