/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.batchlite;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Extend this if the process doesn't write an output file...
 * if all you care about is the status of the process and the
 * stderr and stdout.
 *
 * See {@link FileToFileProcessor} for an abstract class that
 * will process a file and write output to a new file.
 */
public abstract class FileProcessor extends AbstractFileProcessor {

    private final Path srcRoot;
    private final MetadataWriter metadataWriter;

    public FileProcessor(ArrayBlockingQueue<Path> queue,
                         Path srcRoot, MetadataWriter metadataWriter) {
        super(queue);
        this.srcRoot = srcRoot.toAbsolutePath();
        this.metadataWriter = metadataWriter;
    }

    @Override
    public void process(Path srcPath) throws IOException {
        String relPath = srcRoot.relativize(srcPath).toString();
        process(relPath, srcPath, metadataWriter);
    }

    protected abstract void process(String relPath, Path srcPath,
                                    MetadataWriter metadataWriter) throws IOException;
}
