/*
 * Copyright © 2017 Cask Data, Inc.
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


import cirus.io.plugin.format.FileInputFormat;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A source that uses the {@link FileInputFormat} to read whole file as one record.
 *
 * <p>TODO: Move this to hydrator-plugins and make it extends from FileBatchSource to get the full
 * configurability.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("FileListReader")
@Description("Reads the files from the folder and returns file name")
public class FileListSource extends BatchSource<String, String, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(FileListSource.class);

  private final Config config;
  private final Schema outputSchema;

  public FileListSource(Config config) {
    this.config = config;
    this.outputSchema = createOutputSchema();
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    configurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = createJob();
    LOG.info("Reading files from path :{} "+config.path);
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, config.path);
    LOG.info("shouldReadRecursively :{} "+config.recursive);
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputDirRecursive(job, config.recursive);
    final String inputDir = job.getConfiguration().get(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR);
    LOG.info("inputDir :{} "+inputDir);
    context.setInput(
        Input.of(
            config.referenceName,
            new InputFormatProvider() {
              @Override
              public String getInputFormatClassName() {
                return FileInputFormat.class.getName();
              }

              @Override
              public Map<String, String> getInputFormatConfiguration() {
                return Collections.singletonMap(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, inputDir);
              }
            }));
  }

  @Override
  public void transform(KeyValue<String, String> input, Emitter<StructuredRecord> emitter)
      throws Exception {
    emitter.emit(
        StructuredRecord.builder(outputSchema)
            .set("fileName", input.getKey())
            .build());
  }

  private Schema createOutputSchema() {
    return Schema.recordOf(
        "output",
        Schema.Field.of("fileName", Schema.of(Schema.Type.STRING)));
  }

  private static Job createJob() throws IOException {
    try {
      Job job = Job.getInstance();

      LOG.info("Job new instance");

      // some input formats require the credentials to be present in the job. We don't know for
      // sure which ones (HCatalog is one of them), so we simply always add them. This has no other
      // effect, because this method is only used at configure time and will be ignored later on.
      if (UserGroupInformation.isSecurityEnabled()) {
        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        job.getCredentials().addAll(credentials);
      }

      return job;
    } catch (Exception e) {
      LOG.error("Exception ", e);
      throw e;
    }
  }

  /** Configurations for the {@link FileListSource} plugin. */
  public static final class Config extends PluginConfig {

    @Description(
        "This will be used to uniquely identify this source/sink for lineage, annotating metadata, etc.")
    private String referenceName;

    @Description(
        "Path to file(s) to be read. If a directory is specified, "
            + "terminate the path name with a \'/\'. For distributed file system such as HDFS, file system name should come"
            + " from 'fs.DefaultFS' property in the 'core-site.xml'. For example, 'hdfs://mycluster.net:8020/input', where"
            + " value of the property 'fs.DefaultFS' in the 'core-site.xml' is 'hdfs://mycluster.net:8020'. The path uses "
            + "filename expansion (globbing) to read files.")
    @Macro
    private String path;

    @Nullable
    @Description("Whether to recursively read directories within the input directory. The default is false.")
    private Boolean recursive;


    @Override
    public String toString() {
      return "FileListSourceConfig{"
          + "referenceName='"
          + referenceName
          + '\''
          + ", path='"
          + path
          + '\''
          + '}';
    }
  }
}
