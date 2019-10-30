/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Tests to verify configuration of {@link FileListSource} */
public class FileBatchSourceTest extends ETLBatchTestBase {
  public static final String PATH = "path";
  public static final String RECURSIVE = "recursive";
  public static final String SCHEMA = "schema";

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @Test
  public void testRecursiveFolders() throws Exception {
    Schema schema =
        Schema.recordOf(
            "file.record",
            Schema.Field.of("fileName", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Map<String, String> sourceProperties =
        new ImmutableMap.Builder<String, String>()
            .put(Constants.Reference.REFERENCE_NAME, "TestCase")
            .put(PATH, "src/test/resources/")
            .put(RECURSIVE, "true")
            .build();

    ETLStage source =
        new ETLStage(
            "FileListReader", new ETLPlugin("FileListReader", BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "recursive-folders";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig =
        ETLBatchConfig.builder()
            .addStage(source)
            .addStage(sink)
            .addConnection(source.getName(), sink.getName())
            .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("FileTest-recursive-folders");

    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);

    Set<StructuredRecord> expected =
        ImmutableSet.of(
            StructuredRecord.builder(schema)
                .set("offset", 0L)
                .set("body", "Hello,World")
                .set("file", "test1.txt")
                .build(),
            StructuredRecord.builder(schema)
                .set("offset", 0L)
                .set("body", "CDAP,Platform")
                .set("file", "test3.txt")
                .build());
    Set<StructuredRecord> actual = new HashSet<>(MockSink.readOutput(outputManager));
    Assert.assertEquals(expected, actual);
  }

  private ApplicationManager createSourceAndDeployApp(
      String appName, File file, String format, String outputDatasetName, Schema schema)
      throws Exception {
    return createSourceAndDeployApp(appName, file, format, outputDatasetName, schema, null);
  }

  private ApplicationManager createSourceAndDeployApp(
      String appName,
      File file,
      String format,
      String outputDatasetName,
      Schema schema,
      @Nullable String delimiter)
      throws Exception {

    ImmutableMap.Builder<String, String> sourceProperties =
        ImmutableMap.<String, String>builder()
            .put(Constants.Reference.REFERENCE_NAME, appName + "TestFile")
            .put(PATH, file.getAbsolutePath())
            .put("pathField", "file");
    if (delimiter != null) {
      sourceProperties.put("delimiter", delimiter);
    }

    if (schema != null) {
      String schemaString = schema.toString();
      sourceProperties.put(SCHEMA, schemaString);
    }
    ETLStage source =
        new ETLStage(
            "source",
            new ETLPlugin("File", BatchSource.PLUGIN_TYPE, sourceProperties.build(), null));

    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig =
        ETLBatchConfig.builder()
            .addStage(source)
            .addStage(sink)
            .addConnection(source.getName(), sink.getName())
            .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    return deployApplication(appId, appRequest);
  }

  private void verifyDatasetSchema(String dsName, Schema expectedSchema) throws IOException {
    Map<String, String> metadataProperties =
        getMetadataAdmin()
            .getProperties(
                MetadataScope.SYSTEM,
                MetadataEntity.ofDataset(NamespaceId.DEFAULT.getNamespace(), dsName));
    Assert.assertEquals(
        expectedSchema.toString(), metadataProperties.get(DatasetProperties.SCHEMA));
  }
}
