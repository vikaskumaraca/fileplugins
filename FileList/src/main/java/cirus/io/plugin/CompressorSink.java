/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package cirus.io.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Compresses the configured fields using the algorithms specified.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(CompressorSink.NAME)
@Description("Compresses configured fields using the algorithms specified.")
public final class CompressorSink extends SparkSink<StructuredRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(CompressorSink.class);
    private final Config config;
    public static final String NAME = "CompressorSink";

    // Output Schema associated with transform output.
    private Schema outSchema;

    // Output Field name to type map
    private Map<String, Schema.Type> outSchemaMap = new HashMap<>();

    private final Map<String, CompressorType> compMap = new HashMap<>();


    // This is used only for tests, otherwise this is being injected by the ingestion framework.
    public CompressorSink(Config config) {
        this.config = config;
    }

    private void parseConfiguration(String config) throws IllegalArgumentException {
        String[] mappings = config.split(",");
        for (String mapping : mappings) {
            String[] params = mapping.split(":");

            // If format is not right, then we throw an exception.
            if (params.length < 2) {
                throw new IllegalArgumentException("Configuration " + mapping + " is in-correctly formed. " +
                        "Format should be <fieldname>:<compressor-type>");
            }

            String field = params[0];
            String type = params[1].toUpperCase();
            CompressorType cType = CompressorType.valueOf(type);

            if (compMap.containsKey(field)) {
                throw new IllegalArgumentException("Field " + field + " already has compressor set. Check the mapping.");
            } else {
                compMap.put(field, cType);
            }
        }
    }


    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
        super.configurePipeline(pipelineConfigurer);

        Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
        if (inputSchema != null) {
            /*WordCount wordCount = new WordCount(config.field);
            wordCount.validateSchema(inputSchema);*/
        }
        pipelineConfigurer.createDataset(config.tableName, KeyValueTable.class, DatasetProperties.EMPTY);

    }


    private static byte[] gzip(byte[] input) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = null;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(input, 0, input.length);
        } catch (IOException e) {
            // These are all in memory operations, so this should not happen.
            // But, if it happens then we just return null. Logging anything
            // here can be noise.
            return null;
        } finally {
            if (gzip != null) {
                try {
                    gzip.close();
                } catch (IOException e) {
                    return null;
                }
            }
        }

        return out.toByteArray();
    }

    private byte[] zip(byte[] input) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(out);
        try {
            zos.setLevel(9);
            zos.putNextEntry(new ZipEntry("c"));
            zos.write(input, 0, input.length);
            zos.finish();
        } catch (IOException e) {
            return null;
        } finally {
            try {
                if (zos != null) {
                    zos.close();
                }
            } catch (IOException e) {
                return null;
            }
        }

        return out.toByteArray();
    }


    @Override
    public void run(SparkExecutionPluginContext sparkExecutionPluginContext, JavaRDD<StructuredRecord> javaRDD) throws Exception {
        LOG.info("Schema and filds are: " + config.compressor );
        String fieldName = null;
        if (config.compressor != null) {
            String[] split = config.compressor.split(":");
            fieldName = split[0];
        }

        String finalFieldName = fieldName;


        javaRDD.foreach(st -> {

            String fileName = st.get(finalFieldName);
            String outFileName = null;
            if (fileName != null) {
                outFileName = fileName + ".gz";
            }


            String name = fileName;

            try {
                byte[] buffer = new byte[1024];

                FileOutputStream fileOutputStream = new FileOutputStream(outFileName);

                GZIPOutputStream gzipOuputStream = new GZIPOutputStream(fileOutputStream);

                FileInputStream fileInput = new FileInputStream(fileName);

                int bytes_read;

                while ((bytes_read = fileInput.read(buffer)) > 0) {
                    gzipOuputStream.write(buffer, 0, bytes_read);
                }

                fileInput.close();

                gzipOuputStream.finish();
                gzipOuputStream.close();

                System.out.println("The file was compressed successfully!");


            } catch (IOException ex) {
                ex.printStackTrace();
            }

        });


    }

    @Override
    public void prepareRun(SparkPluginContext sparkPluginContext) throws Exception {
        //sparkPluginContext.getInputSchema();
        /*parseConfiguration(config.compressor);
        try {
            outSchema = Schema.parseJson(config.schema);
            List<Schema.Field> outFields = outSchema.getFields();
            for (Schema.Field field : outFields) {
                outSchemaMap.put(field.getName(), field.getSchema().getType());
            }

            for (String field : compMap.keySet()) {
                if (compMap.containsKey(field)) {
                    Schema.Type type = outSchemaMap.get(field);
                    if (type != Schema.Type.BYTES) {
                        throw new IllegalArgumentException("Field '" + field + "' is not of type BYTES. It's currently" +
                                "of type '" + type.toString() + "'.");
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
        }*/
    }


    /**
     * Enum specifying the compressor type.
     */
    private enum CompressorType {
        SNAPPY("SNAPPY"),
        ZIP("ZIP"),
        GZIP("GZIP"),
        NONE("NONE");

        private String type;

        CompressorType(String type) {
            this.type = type;
        }

        String getType() {
            return type;
        }
    }

    /**
     * Plugin configuration.
     */
    public static class Config extends PluginConfig {
        @Name("compressor")
        @Description("Specify the field and compression type combination. " +
                "Format is <field>:<compressor-type>[,<field>:<compressor-type>]*")
        public final String compressor;


        @Name(("tableName"))
        @Description("The name of the KeyValueTable to write to.")
        private String tableName;


        public Config(String compressor, String tableName) {
            this.compressor = compressor;
            this.tableName = tableName;
        }
    }
}
