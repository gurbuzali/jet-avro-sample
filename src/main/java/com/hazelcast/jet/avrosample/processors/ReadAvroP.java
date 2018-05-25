/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.avrosample.processors;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.util.Util;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.reflect.ReflectDatumReader;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Traversers.traverseIterator;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.stream.Collectors.toList;

/**
 * Processor which reads avro files from specified directory using
 * {@link ReflectDatumReader} with given {@code objectClass}
 */
public final class ReadAvroP<R> extends AbstractProcessor {

    private final int parallelism;
    private final int id;
    private final Path directory;
    private final String glob;
    private final Class<R> objectClass;
    private DirectoryStream<Path> directoryStream;
    private Traverser<R> outputTraverser;
    private DataFileReader<R> currentFileReader;

    private ReadAvroP(String directory, String glob, int parallelism, int id, Class<R> objectClass) {
        this.directory = Paths.get(directory);
        this.glob = glob;
        this.parallelism = parallelism;
        this.id = id;
        this.objectClass = objectClass;
    }

    public static <R> ProcessorMetaSupplier metaSupplier(String directory, String glob, Class<R> objectClass) {
        return ProcessorMetaSupplier.of((ProcessorSupplier)
                        count -> IntStream.range(0, count)
                                          .mapToObj(i -> new ReadAvroP<>(directory, glob, count, i, objectClass))
                                          .collect(toList()),
                2);
    }

    @Override
    protected void init(Context ignored) throws Exception {
        directoryStream = Files.newDirectoryStream(directory, glob);
        outputTraverser = Traversers.traverseIterator(directoryStream.iterator())
                                    .filter(this::shouldProcessEvent)
                                    .flatMap(this::processFile);
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(outputTraverser);
    }

    private boolean shouldProcessEvent(Path file) {
        if (Files.isDirectory(file)) {
            return false;
        }
        int hashCode = file.hashCode();
        return ((hashCode & Integer.MAX_VALUE) % parallelism) == id;
    }

    private Traverser<R> processFile(Path file) {
        if (getLogger().isFinestEnabled()) {
            getLogger().finest("Processing file " + file);
        }
        try {
            assert currentFileReader == null : "currentFileReader != null";
            currentFileReader = createFileReader(file);
            return traverseIterator(currentFileReader)
                    .onFirstNull(() -> {
                        Util.uncheckRun(() -> currentFileReader.close());
                        currentFileReader = null;
                    });
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    private DataFileReader<R> createFileReader(Path file) throws IOException {
        return new DataFileReader<>(file.toFile(), new ReflectDatumReader<>(objectClass));
    }

    @Override
    public void close(Throwable error) throws IOException {
        IOException ex = null;
        if (directoryStream != null) {
            try {
                directoryStream.close();
            } catch (IOException e) {
                ex = e;
            }
        }
        if (currentFileReader != null) {
            currentFileReader.close();
        }
        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }
}
