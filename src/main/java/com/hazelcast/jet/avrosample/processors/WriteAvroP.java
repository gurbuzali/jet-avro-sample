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

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.impl.util.Util;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Processor which writes items to the specified avro file using {@link
 * ReflectDatumWriter} with given {@code objectClass}
 */
public class WriteAvroP {

    public static <R> ProcessorMetaSupplier metaSupplier(Class<R> objectClass,
                                                         DistributedSupplier<Schema> schemaSupplier,
                                                         String filename) {
        return ProcessorMetaSupplier.preferLocalParallelismOne(new Supplier<>(objectClass, schemaSupplier, filename));
    }

    private static class Supplier<R> implements ProcessorSupplier {

        Class<R> objectClass;
        DistributedSupplier<Schema> schemaSupplier;
        String filename;

        Supplier(Class<R> objectClass, DistributedSupplier<Schema> schemaSupplier, String filename) {
            this.objectClass = objectClass;
            this.schemaSupplier = schemaSupplier;
            this.filename = filename;
        }

        @Override
        public Collection<? extends Processor> get(int count) {
            DistributedFunction<Processor.Context, DataFileWriter<R>> createFn = context -> {
                try {
                    DataFileWriter<R> writer = new DataFileWriter<>(new ReflectDatumWriter<>(objectClass));
                    writer.create(schemaSupplier.get(), new File(filename + "-" + context.globalProcessorIndex()));
                    return writer;
                } catch (IOException e) {
                    throw ExceptionUtil.sneakyThrow(e);
                }
            };
            DistributedBiConsumer<DataFileWriter<R>, R> onReceiveFn =
                    (writer, item) -> Util.uncheckRun(() -> writer.append(item));
            DistributedConsumer<DataFileWriter<R>> flushFn = writer -> Util.uncheckRun(writer::flush);
            DistributedConsumer<DataFileWriter<R>> destroyFn = writer -> Util.uncheckRun(writer::close);

            DistributedSupplier<Processor> supplier =
                    SinkProcessors.writeBufferedP(createFn, onReceiveFn, flushFn, destroyFn);

            return Stream.generate(supplier).limit(count).collect(toList());
        }
    }

}
