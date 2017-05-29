package com.vmware.xenon.services.common;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.Utils;
import org.openstack4j.api.OSClient;
import org.openstack4j.model.common.Identifier;
import org.openstack4j.model.compute.Image;
import org.openstack4j.model.heat.Stack;
import org.openstack4j.openstack.OSFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * This is an deployment task service. When a new task is posted, it will delete ALL
 * deployment services. It does this in two steps: querying for the deployment services
 * and deleting them.
 *
 * Note that this task service will only be authorized to query or delete deployment
 * services taht are accessible to the user who created the task.
 *
 * This deployment task service embodies the typical task workflow, which looks like:
 *
 * 1. Client does a POST to the task factory service to create a task service
 * 2. The task service will do some work, almost certainly asychronously. (This
 *    task service does queries and DELETEs to other service.)
 * 3. When the work is completed, the task service will update it's state by
 *    PATCHing itself.
 * 4. When the PATCH is received, if more work is to be done, this will be
 *    repeated as needed.
 *
 * You can think of this as proceeding through a state machine. All tasks have a
 * "stage" {@link TaskStage} that covers just the major parts of the lifecycle,
 * such as CREATED, STARTED, and FINISHED. We'll encode the steps that we do as
 * SubStages. For this task service (and most), those sub stages will be associated
 * with just the STARTED TaskStage.
 *
 */
public class DeploymentTaskService
        extends TaskService<DeploymentTaskService.DeploymentTaskServiceState> {

    /**
     * These substages are for tracking the stages unique to our task service. They are only
     * relevant to the STARTED TaskStage. If you create your own task service, these substages
     * will probably be where most of the work happens. See the description above.
     */
    public enum SubStage {
        IMAGE_LIST,
        HEAT_CREATE,
    }

    /** Time in seconds for the task to live */
    private static long DEFAULT_TASK_LIFETIME = 60;

    //public static final String FACTORY_LINK = ServiceUriPaths.CORE + "/deployment-tasks";
    public static final String FACTORY_LINK = "/vio/deployment-tasks";

    /**
     * Create a default factory service that starts instances of this task service on POST.
     */
    public static FactoryService createFactory() {
        return FactoryService.create(DeploymentTaskService.class, ServiceOption.IDEMPOTENT_POST,
                ServiceOption.INSTRUMENTATION);
    }

    public static class DeploymentTaskServiceState extends TaskService.TaskServiceState {

        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String endpoint;

        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String user;

        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String password;

        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String osDomain;
        /**
         * Time in seconds before the task expires
         *
         * Technically, this isn't needed: clients can just set documentExpirationTimeMicros.
         * However, this makes tutorials easier: command-line clients can just set this to
         * a constant instead of calculating a future time
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long taskLifetime;

        /**
         * The current substage. See {@link SubStage}
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public SubStage subStage;

        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String status;

        /**
         * The query we make to the Query Task service, and the result we
         * get back from it.
         */
        @PropertyOptions(usage = {
                PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL,
                PropertyUsageOption.SERVICE_USE })
        public QueryTask deploymentQueryTask;

    }

    public DeploymentTaskService() {
        super(DeploymentTaskServiceState.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.INSTRUMENTATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    /**
     * Ensure that the input task is valid.
     *
     * Technically we don't need to require a body since there are no parameters. However,
     * non-deployment tasks will normally have parameters, so this is an deployment of how they
     * could be validated.
     */
    @Override
    protected DeploymentTaskServiceState validateStartPost(Operation taskOperation) {
        logInfo("********* In DeploymentTaskService.validate ************");
        DeploymentTaskServiceState task = super.validateStartPost(taskOperation);
        if (task == null) {
            return null;
        }

        if (ServiceHost.isServiceCreate(taskOperation)) {
            // apply validation only for the initial creation POST, not restart. Alternatively,
            // this code can exist in the handleCreate method
            if (task.subStage != null) {
                taskOperation.fail(
                        new IllegalArgumentException("Do not specify subStage: internal use only"));
                return null;
            }
            if (task.deploymentQueryTask != null) {
                taskOperation.fail(
                        new IllegalArgumentException(
                                "Do not specify deploymentQueryTask: internal use only"));
                return null;
            }
        }

        if (task.taskLifetime != null && task.taskLifetime <= 0) {
            taskOperation.fail(
                    new IllegalArgumentException("taskLifetime must be positive"));
            return null;
        }

        return task;
    }

    /**
     * Initialize the task
     *
     * We set it to be STARTED: we skip CREATED because we don't need the CREATED state
     * If your task does significant initialization, you may prefer to do it in the
     * CREATED state.
     */
    @Override
    protected void initializeState(DeploymentTaskServiceState task, Operation taskOperation) {
        logInfo("********* In DeploymentTaskService.initialize ************");
        task.subStage = SubStage.IMAGE_LIST;

        if (task.taskLifetime != null) {
            task.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                    + TimeUnit.SECONDS.toMicros(task.taskLifetime);
        } else if (task.documentExpirationTimeMicros != 0) {
            task.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                    + TimeUnit.SECONDS.toMicros(DEFAULT_TASK_LIFETIME);
        }

        // Do our task-specific logic... This will allow our deploymentTaskService's "default"
        // expiration to take precedence over the default expiration set by TaskService
        super.initializeState(task, taskOperation);
    }

    /**
     * Handle PATCH
     *
     * All of the work happens through this task service patching itself. It does
     * an operation (for deployment, querying the deployment services), and when that completes
     * it updates the task state and progresses to the next step by doing a self PATCH.
     *
     */
    @Override
    public void handlePatch(Operation patch) {
        logInfo("********* handlePatch: %s", patch.toString());
        DeploymentTaskServiceState currentTask = getState(patch);
        DeploymentTaskServiceState patchBody = getBody(patch);

        if (!validateTransition(patch, currentTask, patchBody)) {
            return;
        }
        updateState(currentTask, patchBody);
        patch.complete();

        switch (patchBody.taskInfo.stage) {
            case CREATED:
                // Won't happen: validateTransition reports error
                break;
            case STARTED:
                handleSubstage(patchBody);
                break;
            case CANCELLED:
                logInfo("Task canceled: not implemented, ignoring");
                break;
            case FINISHED:
                logFine("Task finished successfully");
                break;
            case FAILED:
                logWarning("Task failed: %s", (patchBody.failureMessage == null ? "No reason given"
                        : patchBody.failureMessage));
                break;
            default:
                logWarning("Unexpected stage: %s", patchBody.taskInfo.stage);
                break;
        }
    }

    private void handleSubstage(DeploymentTaskServiceState task) {
        logInfo("********* In DeploymentTaskService.handleSubstage ************");
        switch (task.subStage) {
            case IMAGE_LIST:
                handleImage(task);
                break;
            case HEAT_CREATE:
                handleHeat(task);
                break;
            default:
                logWarning("Unexpected sub stage: %s", task.subStage);
                break;
        }
    }

    /**
     * Validate that the PATCH we got requests reasonanble changes to our state
     */
    @Override
    protected boolean validateTransition(Operation patch, DeploymentTaskServiceState currentTask,
                                         DeploymentTaskServiceState patchBody) {
        logInfo("********* In DeploymentTaskService.validateTransition ************");
        super.validateTransition(patch, currentTask, patchBody);
        if (patchBody.taskInfo.stage == TaskStage.STARTED && patchBody.subStage == null) {
            patch.fail(new IllegalArgumentException("Missing substage"));
            return false;
        }
        if (currentTask.taskInfo != null && currentTask.taskInfo.stage != null) {
            if (currentTask.taskInfo.stage == TaskStage.STARTED
                    && patchBody.taskInfo.stage == TaskStage.STARTED) {
                if (currentTask.subStage.ordinal() > patchBody.subStage.ordinal()) {
                    patch.fail(new IllegalArgumentException("Task substage cannot move backwards"));
                    return false;
                }
            }
        }

        return true;
    }

    private void handleImage(DeploymentTaskServiceState task) {
        logInfo("********* In DeploymentTaskService.handleImage ************");
        OSClient os = getConnection(task.endpoint, task.user, task.password, task.osDomain);
        List<? extends Image> images = os.compute().images().list();
        task.status = images.toString();
        sendSelfPatch(task, TaskStage.STARTED, subStageSetter(SubStage.HEAT_CREATE));
    }

    private void handleHeat(DeploymentTaskServiceState task) {
        OSClient os = getConnection(task.endpoint, task.user, task.password, task.osDomain);
        List<? extends Stack> stacks = os.heat().stacks().list();
        task.status += stacks.toString();
        sendSelfPatch(task, TaskStage.FINISHED,null);
    }

    private OSClient getConnection(String endpoint, String user, String password, String domain){
        Identifier domainIdentifier = Identifier.byName(domain);
        OSClient os = OSFactory.builderV3()
                .endpoint(endpoint)
                .credentials(user, password, domainIdentifier)
                .scopeToProject(Identifier.byName(user), Identifier.byName(domain))
                .authenticate();
        return os;
    }

    /**
     * Helper method that returns a lambda that will set SubStage for us
     * @param subStage the SubStage to use
     *
     * @return lambda helper needed for {@link TaskService#sendSelfPatch(TaskServiceState, TaskStage, Consumer)}
     */
    private Consumer<DeploymentTaskServiceState> subStageSetter(SubStage subStage) {
        return taskState -> taskState.subStage = subStage;
    }

}

