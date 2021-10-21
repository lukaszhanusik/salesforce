/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.plugin.salesforce.plugin.source.batch;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.salesforce.BulkAPIBatchException;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceBulkUtil;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Batch source to read multiple SObjects from Salesforce.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SalesforceBatchMultiSource.NAME)
@Description("Reads multiple SObjects in Salesforce. "
  + "Outputs one record for each row in each SObject, with the SObject name as a record field. "
  + "Also sets a pipeline argument for each SObject read, which contains its schema.")
public class SalesforceBatchMultiSource extends BatchSource<Schema, Map<String, String>, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceBatchMultiSource.class);
  public static final String NAME = "SalesforceMultiObjects";

  private static final String MULTI_SINK_PREFIX = "multisink.";

  private final SalesforceMultiSourceConfig config;
  private MapToRecordTransformer transformer;
  private BulkConnection bulkConnection;
  private JobInfo job;
  private List<String> jobIds = new ArrayList<>();

  public SalesforceBatchMultiSource(SalesforceMultiSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    config.validate(stageConfigurer.getFailureCollector()); // validate before macros are substituted
    stageConfigurer.setOutputSchema(null);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws ConnectionException {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    List<String> queries = config.getQueries(context.getLogicalStartTime());
    Map<String, Schema> schemas = config.getSObjectsSchemas(queries);

    // propagate schema for each SObject for multi sink plugin
    SettableArguments arguments = context.getArguments();
    schemas.forEach(
      (sObjectName, sObjectSchema) -> arguments.set(MULTI_SINK_PREFIX + sObjectName, sObjectSchema.toString()));

    String sObjectNameField = config.getSObjectNameField();
    // generate splits for queries here and pass it below
    List<SalesforceSplit> querySplits = queries.parallelStream().map(query -> generateSalesforceSplits(query))
      .flatMap(Collection::stream).collect(Collectors.toList());
    context.setInput(Input.of(config.referenceName, new SalesforceInputFormatProvider(
      config, getSchemaWithNameField(sObjectNameField, schemas), querySplits, sObjectNameField)));
    /* TODO PLUGIN-510
     *  As part of [CDAP-16290], recordLineage function was introduced with out implementation.
     *  To avoid compilation errors the code block is commented for future fix.
    Schema schema = context.getInputSchema();
    if (schema != null && schema.getFields() != null) {
      recordLineage(context, config.referenceName, schema,
                    "Read", "Read from Salesforce MultiObjects.");
    }
     */
  }

  private List<SalesforceSplit> generateSalesforceSplits(String query) {
    bulkConnection = getBulkConnection();
    List<SalesforceSplit> querySplits = getQuerySplits(query, bulkConnection, false);
    return querySplits;
  }

  private List<SalesforceSplit> getQuerySplits(String query, BulkConnection bulkConnection, boolean enablePKChunk) {
    return Stream.of(getBatches(query, bulkConnection, enablePKChunk))
      .map(batch -> new SalesforceSplit(batch.getJobId(), batch.getId(), query))
      .collect(Collectors.toList());
  }

  /**
   * Based on query length sends query to Salesforce to receive array of batch info. If query is within limit, executes
   * original query. If not, switches to wide object logic, i.e. generates Id query to retrieve batch info for Ids only
   * that will be used later to retrieve data using SOAP API.
   *
   * @param query SOQL query
   * @param bulkConnection bulk connection
   * @param enablePKChunk enable PK Chunking
   * @return array of batch info
   */
  private BatchInfo[] getBatches(String query, BulkConnection bulkConnection, boolean enablePKChunk) {
    try {
      if (!SalesforceQueryUtil.isQueryUnderLengthLimit(query)) {
        LOG.debug("Wide object query detected. Query length '{}'", query.length());
        query = SalesforceQueryUtil.createSObjectIdQuery(query);
      }
      BatchInfo[] batches = runBulkQuery(bulkConnection, query, enablePKChunk);
      LOG.debug("Number of batches received from Salesforce: '{}'", batches.length);
      return batches;
    } catch (AsyncApiException | IOException e) {
      throw new RuntimeException("There was issue communicating with Salesforce", e);
    }
  }

  /**
   * Start batch job of reading a given guery result.
   *
   * @param bulkConnection bulk connection instance
   * @param query a SOQL query
   * @param enablePKChunk enable PK Chunk
   * @return an array of batches
   * @throws AsyncApiException  if there is an issue creating the job
   * @throws IOException failed to close the query
   */
  public BatchInfo[] runBulkQuery(BulkConnection bulkConnection, String query, boolean enablePKChunk)
    throws AsyncApiException, IOException {

    SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(query);
    job = SalesforceBulkUtil.createJob(bulkConnection, sObjectDescriptor.getName(), OperationEnum.query, null);
    jobIds.add(job.getId());
    LOG.info("List of JobIds {}", jobIds);
    BatchInfo batchInfo;
    try (ByteArrayInputStream bout = new ByteArrayInputStream(query.getBytes())) {
      batchInfo = bulkConnection.createBatchFromStream(job, bout);
    }

    if (enablePKChunk) {
      LOG.info("PKChunk is enabled, returning something now");
      return waitForBatchChunks(bulkConnection, job.getId(), batchInfo.getId());
    }
    LOG.info("PKChunk is not enabled, getting batch info list");
    BatchInfo[] batchInfos = bulkConnection.getBatchInfoList(job.getId()).getBatchInfo();
    LOG.info("Job id {}, status: {}", job.getId(), bulkConnection.getJobStatus(job.getId()).getState());
    if (batchInfos.length > 0) {
      LOG.info("Batch size {}, state {}", batchInfos.length, batchInfos[0].getState());
    }
    return batchInfos;
  }

  /**
   * Initializes bulk connection based on given Hadoop configuration.
   *
   * @return bulk connection instance
   */
  private BulkConnection getBulkConnection() {
    try {
      AuthenticatorCredentials credentials = SalesforceConnectionUtil.getAuthenticatorCredentials(
        config.getUsername(), config.getPassword(), config.getConsumerKey(), config.getConsumerSecret(),
        config.getLoginUrl()
      );
      return new BulkConnection(Authenticator.createConnectorConfig(credentials));
    } catch (AsyncApiException e) {
      throw new RuntimeException("There was issue communicating with Salesforce", e);
    }
  }

  /** When PK Chunk is enabled, wait for state of initial batch to be NotProcessed, in this case Salesforce API will
   * decide how many batches will be created
   * @param bulkConnection bulk connection instance
   * @param jobId a job id
   * @param initialBatchId a batch id
   * @return Array with Batches created by Salesforce API
   *
   * @throws AsyncApiException if there is an issue creating the job
   */
  private BatchInfo[] waitForBatchChunks(BulkConnection bulkConnection, String jobId, String initialBatchId)
    throws AsyncApiException {
    BatchInfo initialBatchInfo = null;
    for (int i = 0; i < SalesforceSourceConstants.GET_BATCH_RESULTS_TRIES; i++) {
      //check if the job is aborted
      if (bulkConnection.getJobStatus(jobId).getState() == JobStateEnum.Aborted) {
        LOG.info(String.format("Job with Id: '%s' is aborted", jobId));
        return new BatchInfo[0];
      }
      try {
        initialBatchInfo = bulkConnection.getBatchInfo(jobId, initialBatchId);
      } catch (AsyncApiException e) {
        if (i == SalesforceSourceConstants.GET_BATCH_RESULTS_TRIES - 1) {
          throw e;
        }
        LOG.warn("Failed to get info for batch {}. Will retry after some time.", initialBatchId, e);
        continue;
      }

      if (initialBatchInfo.getState() == BatchStateEnum.NotProcessed) {
        BatchInfo[] result = bulkConnection.getBatchInfoList(jobId).getBatchInfo();
        return Arrays.stream(result).filter(batchInfo -> batchInfo.getState() != BatchStateEnum.NotProcessed)
          .toArray(BatchInfo[]::new);
      } else if (initialBatchInfo.getState() == BatchStateEnum.Failed) {
        throw new BulkAPIBatchException("Batch failed", initialBatchInfo);
      } else {
        try {
          Thread.sleep(SalesforceSourceConstants.GET_BATCH_RESULTS_SLEEP_MS);
        } catch (InterruptedException e) {
          throw new RuntimeException("Job is aborted", e);
        }
      }
    }
    throw new BulkAPIBatchException("Timeout waiting for batch results", initialBatchInfo);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    this.transformer = new MapToRecordTransformer();
  }

  @Override
  public void transform(KeyValue<Schema, Map<String, String>> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord record = transformer.transform(input.getKey(), input.getValue());
    emitter.emit(record);
  }

  /**
   * For each given schema adds name field of type String and converts it to string representation.
   *
   * @param sObjectNameField sObject field name
   * @param schemas map of schemas where key is SObject name to which value schema corresponds
   * @return schema with named field
   */
  private Map<String, String> getSchemaWithNameField(String sObjectNameField, Map<String, Schema> schemas) {
    return schemas.entrySet().stream()
      .collect(Collectors.toMap(
        Map.Entry::getKey,
        entry -> getSchemaString(sObjectNameField, entry.getValue()),
        (o, n) -> n));
  }

  /**
   * Adds sObject name field to the given schema and converts it to string representation.
   *
   * @param sObjectNameField sObject name field
   * @param schema CDAP schema
   * @return updated schema in string representation
   */
  private String getSchemaString(String sObjectNameField, Schema schema) {
    if (schema.getType() != Schema.Type.RECORD || schema.getFields() == null) {
      throw new IllegalArgumentException(String.format("Invalid schema '%s'", schema));
    }
    List<Schema.Field> fields = new ArrayList<>(schema.getFields());
    fields.add(Schema.Field.of(sObjectNameField, Schema.of(Schema.Type.STRING)));
    return Schema.recordOf(Objects.requireNonNull(schema.getRecordName()), fields).toString();
  }
}
