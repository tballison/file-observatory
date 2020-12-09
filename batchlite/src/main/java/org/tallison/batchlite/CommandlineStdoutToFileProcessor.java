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

public abstract class CommandlineStdoutToFileProcessor extends FileToFileProcessor {
    public CommandlineStdoutToFileProcessor(ArrayBlockingQueue<Path> queue,
                                            Path srcRoot, Path targRoot,
                                            MetadataWriter metadataWriter) {
        super(queue, srcRoot, targRoot, metadataWriter);
    }

    @Override
    protected void process(String relPath,
                                    Path srcPath, Path outputPath, MetadataWriter metadataWriter) throws IOException {
        String[] commandline = getCommandLine(srcPath);
        FileProcessResult r = ProcessExecutor.execute(new ProcessBuilder(commandline),
                getFileTimeoutMillis(), outputPath,
                metadataWriter.getMaxStderrBuffer());
        metadataWriter.write(relPath, r);
     }

    protected abstract String[] getCommandLine(Path srcPath) throws IOException;

}
