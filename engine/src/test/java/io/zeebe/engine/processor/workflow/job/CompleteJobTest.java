/*
 * Zeebe Workflow Engine
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.engine.processor.workflow.job;

import static io.zeebe.test.util.record.RecordingExporter.jobRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.ThrowableAssert.catchThrowable;

import io.zeebe.engine.processor.workflow.MsgPackConstants;
import io.zeebe.engine.util.EngineRule;
import io.zeebe.exporter.api.record.Assertions;
import io.zeebe.exporter.api.record.Record;
import io.zeebe.exporter.api.record.RecordMetadata;
import io.zeebe.exporter.api.record.value.JobBatchRecordValue;
import io.zeebe.exporter.api.record.value.JobRecordValue;
import io.zeebe.msgpack.spec.MsgPackHelper;
import io.zeebe.protocol.RecordType;
import io.zeebe.protocol.RejectionType;
import io.zeebe.protocol.impl.encoding.MsgPackConverter;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.test.util.MsgPackUtil;
import io.zeebe.test.util.Strings;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class CompleteJobTest {
  private static final String JSON_VARIABLES = "{\"foo\":\"bar\"}";
  private static final byte[] VARIABLES_MSG_PACK = MsgPackUtil.asMsgPackReturnArray(JSON_VARIABLES);
  private static final String PROCESS_ID = "process";
  private static String jobType;

  @ClassRule public static EngineRule engineRule = new EngineRule();

  @Before
  public void setup() {
    jobType = Strings.newRandomValidBpmnId();
  }

  @Test
  public void shouldCompleteJob() {
    // given
    engineRule.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = engineRule.jobs().withType(jobType).activate();
    final JobRecordValue job = batchRecord.getValue().getJobs().get(0);

    // when
    final Record<JobRecordValue> jobCompletedRecord =
        engineRule
            .job()
            .withVariables(MsgPackConstants.MSGPACK_VARIABLES)
            .complete(batchRecord.getValue().getJobKeys().get(0));

    // then
    final RecordMetadata metadata = jobCompletedRecord.getMetadata();
    final JobRecordValue recordValue = jobCompletedRecord.getValue();

    assertThat(metadata.getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(metadata.getIntent()).isEqualTo(JobIntent.COMPLETED);

    Assertions.assertThat(recordValue)
        .hasWorker(batchRecord.getValue().getWorker())
        .hasType(job.getType())
        .hasRetries(job.getRetries())
        .hasDeadline(job.getDeadline());
  }

  @Test
  public void shouldRejectCompletionIfJobNotFound() {
    // given
    final int key = 123;

    // when
    final Record<JobRecordValue> jobRecord =
        engineRule
            .job()
            .withVariables(MsgPackConstants.MSGPACK_VARIABLES)
            .expectRejection()
            .complete(key);

    // then
    Assertions.assertThat(jobRecord.getMetadata()).hasRejectionType(RejectionType.NOT_FOUND);
  }

  @Test
  public void shouldCompleteJobWithVariables() {
    // given
    engineRule.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = engineRule.jobs().withType(jobType).activate();

    // when
    final Record<JobRecordValue> completedRecord =
        engineRule
            .job()
            .withVariables(MsgPackConstants.MSGPACK_VARIABLES)
            .complete(batchRecord.getValue().getJobKeys().get(0));

    // then
    Assertions.assertThat(completedRecord.getMetadata())
        .hasRecordType(RecordType.EVENT)
        .hasIntent(JobIntent.COMPLETED);
    assertThat(completedRecord.getValue().getVariables())
        .isEqualTo(MsgPackConverter.convertToJson(MsgPackConstants.MSGPACK_VARIABLES));
  }

  @Test
  public void shouldCompleteJobWithNilVariables() {
    // given
    engineRule.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = engineRule.jobs().withType(jobType).activate();

    // when
    final Record<JobRecordValue> completedRecord =
        engineRule
            .job()
            .withVariables(new UnsafeBuffer(MsgPackHelper.NIL))
            .complete(batchRecord.getValue().getJobKeys().get(0));

    // then
    Assertions.assertThat(completedRecord.getMetadata())
        .hasRecordType(RecordType.EVENT)
        .hasIntent(JobIntent.COMPLETED);
    assertThat(MsgPackConverter.convertToMsgPack(completedRecord.getValue().getVariables()))
        .isEqualTo(MsgPackHelper.EMTPY_OBJECT);
  }

  @Test
  public void shouldCompleteJobWithZeroLengthVariables() {
    // given
    engineRule.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = engineRule.jobs().withType(jobType).activate();

    // when
    final Record<JobRecordValue> completedRecord =
        engineRule
            .job()
            .withVariables(new UnsafeBuffer(new byte[0]))
            .complete(batchRecord.getValue().getJobKeys().get(0));

    // then
    Assertions.assertThat(completedRecord.getMetadata())
        .hasRecordType(RecordType.EVENT)
        .hasIntent(JobIntent.COMPLETED);
    assertThat(MsgPackConverter.convertToMsgPack(completedRecord.getValue().getVariables()))
        .isEqualTo(MsgPackHelper.EMTPY_OBJECT);
  }

  @Test
  public void shouldCompleteJobWithNoVariables() {
    // given
    final Record<JobRecordValue> job = engineRule.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = engineRule.jobs().withType(jobType).activate();
    final Record<JobRecordValue> activated =
        jobRecords(JobIntent.ACTIVATED).withRecordKey(job.getKey()).getFirst();

    // when
    final DirectBuffer variables = MsgPackUtil.asMsgPack(activated.getValue().getVariables());
    final Record<JobRecordValue> completedRecord =
        engineRule.job().withVariables(variables).complete(activated.getKey());

    // then
    Assertions.assertThat(completedRecord.getMetadata())
        .hasRecordType(RecordType.EVENT)
        .hasIntent(JobIntent.COMPLETED);
    MsgPackUtil.assertEquality(
        MsgPackHelper.EMTPY_OBJECT, completedRecord.getValue().getVariables());
  }

  @Test
  public void shouldThrowExceptionOnCompletionIfVariablesAreInvalid() {
    // given
    engineRule.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = engineRule.jobs().withType(jobType).activate();

    final byte[] invalidVariables = new byte[] {1}; // positive fixnum, i.e. no object

    // when
    final Throwable throwable =
        catchThrowable(
            () ->
                engineRule
                    .job()
                    .withVariables(new UnsafeBuffer(invalidVariables))
                    .expectRejection()
                    .complete(batchRecord.getValue().getJobKeys().get(0)));

    // then
    assertThat(throwable).isInstanceOf(RuntimeException.class);
    assertThat(throwable.getMessage()).contains("Property 'variables' is invalid");
    assertThat(throwable.getMessage())
        .contains("Expected document to be a root level object, but was 'INTEGER'");
  }

  @Test
  public void shouldRejectCompletionIfJobIsCompleted() {
    // given
    engineRule.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = engineRule.jobs().withType(jobType).activate();

    final DirectBuffer variables =
        MsgPackUtil.asMsgPack(batchRecord.getValue().getJobs().get(0).getVariables());
    final Long jobKey = batchRecord.getValue().getJobKeys().get(0);
    engineRule.job().withVariables(variables).complete(jobKey);

    // when
    final Record<JobRecordValue> jobRecord =
        engineRule.job().withVariables(variables).expectRejection().complete(jobKey);

    // then
    Assertions.assertThat(jobRecord.getMetadata()).hasRejectionType(RejectionType.NOT_FOUND);
  }

  @Test
  public void shouldRejectCompletionIfJobIsFailed() {
    // given
    engineRule.createJob(jobType, PROCESS_ID);

    // when
    final Record<JobBatchRecordValue> batchRecord = engineRule.jobs().withType(jobType).activate();
    final Long jobKey = batchRecord.getValue().getJobKeys().get(0);
    engineRule.job().fail(jobKey);

    final Record<JobRecordValue> jobRecord =
        engineRule
            .job()
            .withVariables(
                MsgPackUtil.asMsgPack(batchRecord.getValue().getJobs().get(0).getVariables()))
            .expectRejection()
            .complete(jobKey);

    // then
    Assertions.assertThat(jobRecord.getMetadata()).hasRejectionType(RejectionType.INVALID_STATE);
  }
}