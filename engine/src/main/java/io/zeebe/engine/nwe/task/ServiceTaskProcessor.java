package io.zeebe.engine.nwe.task;

import io.zeebe.engine.Loggers;
import io.zeebe.engine.nwe.BpmnElementContext;
import io.zeebe.engine.nwe.BpmnElementProcessor;
import io.zeebe.engine.nwe.behavior.BpmnBehaviors;
import io.zeebe.engine.nwe.behavior.BpmnIncidentBehavior;
import io.zeebe.engine.nwe.behavior.BpmnStateBehavior;
import io.zeebe.engine.processor.Failure;
import io.zeebe.engine.processor.TypedCommandWriter;
import io.zeebe.engine.processor.workflow.CatchEventBehavior;
import io.zeebe.engine.processor.workflow.ExpressionProcessor;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableServiceTask;
import io.zeebe.engine.processor.workflow.handlers.IOMappingHelper;
import io.zeebe.engine.state.instance.JobState.State;
import io.zeebe.msgpack.value.DocumentValue;
import io.zeebe.protocol.impl.record.value.job.JobRecord;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.util.Either;

public final class ServiceTaskProcessor implements BpmnElementProcessor<ExecutableServiceTask> {

  private final JobRecord jobCommand = new JobRecord().setVariables(DocumentValue.EMPTY_DOCUMENT);

  private final IOMappingHelper variableMappingBehavior;
  private final CatchEventBehavior eventSubscriptionBehavior;
  private final ExpressionProcessor expressionBehavior;
  private final TypedCommandWriter commandWriter;
  private final BpmnIncidentBehavior incidentBehavior;
  private final BpmnStateBehavior stateBehavior;

  public ServiceTaskProcessor(final BpmnBehaviors behaviors) {
    variableMappingBehavior = behaviors.variableMappingBehavior();
    eventSubscriptionBehavior = behaviors.eventSubscriptionBehavior();
    expressionBehavior = behaviors.expressionBehavior();
    commandWriter = behaviors.commandWriter();
    incidentBehavior = behaviors.incidentBehavior();
    stateBehavior = behaviors.stateBehavior();
  }

  @Override
  public Class<ExecutableServiceTask> getType() {
    return ExecutableServiceTask.class;
  }

  @Override
  public void onActivating(final ExecutableServiceTask element, final BpmnElementContext context) {
    // for all activities:
    // input mappings
    // subscribe to events

    variableMappingBehavior.applyInputMappings(context.toStepContext());
    eventSubscriptionBehavior.subscribeToEvents(context.toStepContext(), element);
  }

  @Override
  public void onActivated(final ExecutableServiceTask element, final BpmnElementContext context) {
    // only for service task:
    // evaluate job type expression
    // evaluate job retries expression
    // create job

    // --> may be better done on activating

    final Either<Failure, String> optJobType =
        expressionBehavior.evaluateStringExpression(
            element.getType(), context.getElementInstanceKey());

    final Either<Failure, Long> optRetries =
        expressionBehavior.evaluateLongExpression(
            element.getRetries(), context.getElementInstanceKey());

    if (optJobType.isRight() && optRetries.isRight()) {
      createNewJob(context, element, optJobType.get(), optRetries.get().intValue());
    }
  }

  @Override
  public void onCompleting(final ExecutableServiceTask element, final BpmnElementContext context) {
    // for all activities:
    // output mappings
    // unsubscribe from events

    variableMappingBehavior.applyOutputMappings(context.toStepContext());
    eventSubscriptionBehavior.unsubscribeFromEvents(
        context.getElementInstanceKey(), context.toStepContext());
  }

  @Override
  public void onCompleted(final ExecutableServiceTask element, final BpmnElementContext context) {
    // for all activities:
    // take outgoing sequence flows
    // complete scope if last active token
    // consume token
    // remove from event scope instance state
    // remove from element instance state
  }

  @Override
  public void onTerminating(final ExecutableServiceTask element, final BpmnElementContext context) {
    // only for service task:
    // cancel job
    // resolve job incident

    // for all activities:
    // unsubscribe from events

    final var elementInstance = stateBehavior.getElementInstance(context);
    final long jobKey = elementInstance.getJobKey();
    if (jobKey > 0) {
      cancelJob(jobKey);
      incidentBehavior.resolveJobIncident(jobKey);
    }

    eventSubscriptionBehavior.unsubscribeFromEvents(
        context.getElementInstanceKey(), context.toStepContext());
  }

  @Override
  public void onTerminated(final ExecutableServiceTask element, final BpmnElementContext context) {
    // for all activities:
    // publish deferred events (i.e. an occurred boundary event)
    // resolve incidents
    // terminate scope if scope is terminated and last active token
    // publish deferred event if an interrupting event sub-process was triggered
    // consume token
  }

  @Override
  public void onEventOccurred(
      final ExecutableServiceTask element, final BpmnElementContext context) {
    // for all activities:
    // (when boundary event is triggered)
    // if interrupting then terminate element and defer occurred event
    // if non-interrupting then activate boundary event, remove event trigger from state, spawn
    // token
  }

  private void createNewJob(
      final BpmnElementContext context,
      final ExecutableServiceTask serviceTask,
      final String jobType,
      final int retries) {

    jobCommand
        .setType(jobType)
        .setRetries(retries)
        .setCustomHeaders(serviceTask.getEncodedHeaders())
        .setBpmnProcessId(context.getBpmnProcessId())
        .setWorkflowDefinitionVersion(context.getWorkflowVersion())
        .setWorkflowKey(context.getWorkflowKey())
        .setWorkflowInstanceKey(context.getWorkflowInstanceKey())
        .setElementId(serviceTask.getId())
        .setElementInstanceKey(context.getElementInstanceKey());

    commandWriter.appendNewCommand(JobIntent.CREATE, jobCommand);
  }

  private void cancelJob(final long jobKey) {
    final State state = stateBehavior.getJobState().getState(jobKey);

    if (state == State.NOT_FOUND) {
      Loggers.WORKFLOW_PROCESSOR_LOGGER.warn(
          "Expected to find job with key {}, but no job found", jobKey);

    } else if (state == State.ACTIVATABLE || state == State.ACTIVATED || state == State.FAILED) {
      final JobRecord job = stateBehavior.getJobState().getJob(jobKey);
      commandWriter.appendFollowUpCommand(jobKey, JobIntent.CANCEL, job);
    }
  }
}
