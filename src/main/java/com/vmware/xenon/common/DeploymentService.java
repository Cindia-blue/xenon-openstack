package com.vmware.xenon.services.common;

import com.vmware.xenon.common.*;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.services.common.DeploymentTaskService.DeploymentTaskServiceState;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Deployment service
 */
public class DeploymentService extends StatefulService {

    //public static final String FACTORY_LINK = ServiceUriPaths.CORE + "/deployments";
    public static final String FACTORY_LINK = "/oms/deployments";

    /**
     * Create a default factory service that starts instances of this service on POST.
     * This method is optional, {@code FactoryService.create} can be used directly
     */
    public static FactoryService createFactory() {
        return FactoryService.createIdempotent(DeploymentService.class);
    }

    public static class DeploymentServiceState extends ServiceDocument {
        public static final String FIELD_NAME_KEY_VALUES = "keyValues";
        public static final String FIELD_NAME_COUNTER = "counter";
        public static final String FIELD_NAME_NAME = "name";
        public static final long VERSION_RETENTION_LIMIT = 100;

        @UsageOption(option = PropertyUsageOption.OPTIONAL)
        public Map<String, String> keyValues = new HashMap<>();
        public Long counter;
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String name;
        @UsageOption(option = PropertyUsageOption.OPTIONAL)
        public String endpoint;
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String status;
    }

    public DeploymentService() {
        super(DeploymentServiceState.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.INSTRUMENTATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleStart(Operation startPost) {
        // Deployment of state validation on start:
        // 1) Require that an initial state is provided
        // 2) Require that the name field is not null
        // A service could also accept a POST with no body or invalid state and correct it
        logInfo("********* In DeploymentService.handleStart ************");

        if (!startPost.hasBody()) {
            startPost.fail(new IllegalArgumentException("initial state is required"));
            return;
        }

        DeploymentServiceState s = startPost.getBody(DeploymentServiceState.class);
        if (s.name == null) {
            startPost.fail(new IllegalArgumentException("name is required"));
            return;
        }

        if (s.status == null) {
            s.status = "init";
            URI deploymentTaskFactoryUri = UriUtils.buildFactoryUri(this.getHost(), DeploymentTaskService.class);
            logInfo("********* In DeploymentService: %s", deploymentTaskFactoryUri.toString());

            DeploymentTaskServiceState task = new DeploymentTaskServiceState();
            task.endpoint = s.endpoint;
            task.documentSelfLink = task.endpoint;
            Operation createPost = Operation.createPost(deploymentTaskFactoryUri)
                    .setBody(task)
                    .setCompletion(
                            (op, ex) -> {
                                if (ex != null) {
                                    logWarning("Deployment failed, task will not finish: %s",
                                            ex.getMessage());
                                    return;
                                }
                            });

            sendRequest(createPost);
            startPost.addResponseHeader("Location", UriUtils.buildUri(this.getHost(), "oms/deployments/" + task.endpoint).getPath());
            startPost.setStatusCode(Operation.STATUS_CODE_ACCEPTED);
            startPost.complete();
        } else {
            startPost.complete();
        }
    }

    @Override
    public void handlePut(Operation put) {
        logInfo("********* In DeploymentService.handlePut ************");
        DeploymentServiceState newState = getBody(put);
        DeploymentServiceState currentState = getState(put);

        // Deployment of structural validation: check if the new state is acceptable
        if (currentState.name != null && newState.name == null) {
            put.fail(new IllegalArgumentException("name must be set"));
            return;
        }

        updateCounter(newState, currentState, false);

        // replace current state, with the body of the request, in one step
        setState(put, newState);
        put.complete();
    }

    @Override
    public void handlePatch(Operation patch) {
        logInfo("********* In DeploymentService.handlePatch ************");
        updateState(patch);
        // updateState method already set the response body with the merged state
        patch.complete();
    }

    private DeploymentServiceState updateState(Operation update) {
        // A DCP service handler is state-less: Everything it needs is provided as part of the
        // of the operation. The body and latest state associated with the service are retrieved
        // below.
        DeploymentServiceState body = getBody(update);
        DeploymentServiceState currentState = getState(update);

        // use helper that will merge automatically current state, with state supplied in body.
        // Note the usage option PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL has been set on the
        // "name" field.
        boolean hasStateChanged = Utils.mergeWithState(getDocumentTemplate().documentDescription,
                currentState, body);

        updateCounter(body, currentState, hasStateChanged);
        if (body.keyValues != null && !body.keyValues.isEmpty()) {
            for (Entry<String, String> e : body.keyValues.entrySet()) {
                currentState.keyValues.put(e.getKey(), e.getValue());
            }
        }

        if (body.documentExpirationTimeMicros != currentState.documentExpirationTimeMicros) {
            currentState.documentExpirationTimeMicros = body.documentExpirationTimeMicros;
        }

        currentState.status = body.status;
        currentState.endpoint = body.endpoint;

        // response has latest, updated state
        update.setBody(currentState);
        return currentState;
    }

    private boolean updateCounter(DeploymentServiceState body,
                                  DeploymentServiceState currentState, boolean hasStateChanged) {
        if (body.counter != null) {
            if (currentState.counter == null) {
                currentState.counter = body.counter;
            }
            // deal with possible operation re-ordering by simply always
            // moving the counter up
            currentState.counter = Math.max(body.counter, currentState.counter);
            body.counter = currentState.counter;
            hasStateChanged = true;
        }
        return hasStateChanged;
    }

    /**
     * Provides a default instance of the service state and allows service author to specify
     * indexing and usage options, per service document property
     */
    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocument template = super.getDocumentTemplate();
        PropertyDescription pd = template.documentDescription.propertyDescriptions.get(
                DeploymentServiceState.FIELD_NAME_KEY_VALUES);

        // instruct the index to deeply index the map
        pd.indexingOptions.add(PropertyIndexingOption.EXPAND);

        PropertyDescription pdName = template.documentDescription.propertyDescriptions.get(
                DeploymentServiceState.FIELD_NAME_NAME);

        // instruct the index to enable SORT on this field.
        pdName.indexingOptions.add(PropertyIndexingOption.SORT);

        // instruct the index to only keep the most recent N versions
        template.documentDescription.versionRetentionLimit = DeploymentServiceState.VERSION_RETENTION_LIMIT;
        return template;
    }
}
