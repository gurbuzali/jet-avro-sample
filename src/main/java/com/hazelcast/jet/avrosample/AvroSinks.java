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

package com.hazelcast.jet.avrosample;

import com.hazelcast.jet.avrosample.processors.WriteAvroP;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.apache.avro.Schema;

public class AvroSinks {



    public static <R> Sink<R> avroFiles(Class<R> objectClass,
                                        DistributedSupplier<Schema> schemaSupplier,
                                        String filename) {
        return Sinks.fromProcessor("avroSink", WriteAvroP.metaSupplier(objectClass, schemaSupplier, filename));
    }

}
