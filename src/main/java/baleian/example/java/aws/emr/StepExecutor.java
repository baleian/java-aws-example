package baleian.example.java.aws.emr;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.waiters.WaiterHandler;
import com.amazonaws.waiters.WaiterParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StepExecutor {
    private static Logger logger = LoggerFactory.getLogger(StepExecutor.class);

    private AmazonElasticMapReduce client;

    public AmazonElasticMapReduce getClient() {
        return client;
    }

    public void setClient(AmazonElasticMapReduce client) {
        this.client = client;
    }

    public List<Step> execute(String clusterId, Collection<StepConfig> stepConfigs) {
        return execute(clusterId, stepConfigs, null);
    }

    public List<Step> execute(ClusterConfig clusterConfig, Collection<StepConfig> stepConfigs) {
        return execute(createTemporaryCluster(clusterConfig), stepConfigs);
    }

    public List<Step> execute(String clusterId, Collection<StepConfig> stepConfigs, final StepExecutorCallback callback) {
        List<String> stepIds = executeAsync(clusterId, stepConfigs, callback);
        List<Step> steps = new ArrayList<Step>();
        for (String stepId : stepIds) {
            final DescribeStepRequest stepRequest = new DescribeStepRequest().withClusterId(clusterId).withStepId(stepId);
            logger.info("Waiting step: {}", client.describeStep(stepRequest).getStep());

            try {
                client.waiters().stepComplete().run(new WaiterParameters<DescribeStepRequest>(stepRequest));
            } catch (Exception e) {
                logger.error("waiting execution exception", e);
            }

            Step step = client.describeStep(stepRequest).getStep();
            logger.info("Finished step: {}", step);
            steps.add(step);
        }
        return steps;
    }

    public List<Step> execute(ClusterConfig clusterConfig, Collection<StepConfig> stepConfigs, final StepExecutorCallback callback) {
        return execute(createTemporaryCluster(clusterConfig), stepConfigs, callback);
    }

    public List<String> executeAsync(String clusterId, Collection<StepConfig> steps, final StepExecutorCallback callback) {
        AddJobFlowStepsResult result = client.addJobFlowSteps(new AddJobFlowStepsRequest()
                .withJobFlowId(clusterId)
                .withSteps(steps));

        List<String> stepIds = result.getStepIds();

        if (callback == null) {
            return stepIds;
        }

        for (final String stepId : stepIds) {
            final DescribeStepRequest stepRequest = new DescribeStepRequest().withClusterId(clusterId).withStepId(stepId);
            client.waiters().stepComplete().runAsync(new WaiterParameters<DescribeStepRequest>(stepRequest), new WaiterHandler() {
                @Override
                public void onWaitSuccess(AmazonWebServiceRequest amazonWebServiceRequest) {
                    Step step = client.describeStep(stepRequest).getStep();
                    logger.debug("onWaitSuccess step: {}", step);

                    if ("COMPLETED".equals(step.getStatus().getState())) {
                        callback.onSuccee(step);
                    }
                    else {
                        callback.onFailure(step);
                    }
                }

                @Override
                public void onWaitFailure(Exception e) {
                    Step step = client.describeStep(stepRequest).getStep();
                    logger.debug("onWaitFailure step: {}", step);
                    callback.onError(step, e);
                }
            });
        }

        return stepIds;
    }

    public List<String> executeAsync(ClusterConfig clusterConfig, Collection<StepConfig> steps, final StepExecutorCallback callback) {
        return executeAsync(createTemporaryCluster(clusterConfig), steps, callback);
    }

    private String createTemporaryCluster(ClusterConfig clusterConfig) {
        RunJobFlowResult runJobFlowResult = client.runJobFlow(createRunJobFlowRequest(clusterConfig));
        String clusterId = runJobFlowResult.getJobFlowId();

        final DescribeClusterRequest describeClusterRequest = new DescribeClusterRequest().withClusterId(clusterId);
        logger.info("creating cluster: {}", client.describeCluster(describeClusterRequest).getCluster());

        client.waiters().clusterRunning().runAsync(new WaiterParameters<DescribeClusterRequest>().withRequest(describeClusterRequest), new WaiterHandler() {
            @Override
            public void onWaitSuccess(AmazonWebServiceRequest amazonWebServiceRequest) {
                logger.info("running cluster: {}", client.describeCluster(describeClusterRequest).getCluster());
            }

            @Override
            public void onWaitFailure(Exception e) {
                logger.error("fail to create cluster", e);
            }
        });

        client.waiters().clusterTerminated().runAsync(new WaiterParameters<DescribeClusterRequest>(describeClusterRequest), new WaiterHandler() {
            @Override
            public void onWaitSuccess(AmazonWebServiceRequest amazonWebServiceRequest) {
                logger.info("terminated cluster: {}", client.describeCluster(describeClusterRequest).getCluster());
            }

            @Override
            public void onWaitFailure(Exception e) {
                logger.error("fail to terminate cluster", e);
            }
        });

        return clusterId;
    }

    private RunJobFlowRequest createRunJobFlowRequest(ClusterConfig clusterConfig) {
        return new RunJobFlowRequest()
                .withName(clusterConfig.getClusterName())
                .withJobFlowRole(clusterConfig.getJobFlowRole())
                .withServiceRole(clusterConfig.getServiceRole())
                .withAutoScalingRole(clusterConfig.getAutoScalingRole())
                .withReleaseLabel(clusterConfig.getReleaseLabel())
                .withLogUri(clusterConfig.getLogUri())
                .withApplications(clusterConfig.getApplications())
                .withVisibleToAllUsers(clusterConfig.isVisibleToAllUsers())
                .withInstances(new JobFlowInstancesConfig()
                        .withHadoopVersion(clusterConfig.getHadoopVersion())
                        .withInstanceCount(clusterConfig.getInstanceCount())
                        .withMasterInstanceType(clusterConfig.getMasterInstanceType())
                        .withSlaveInstanceType(clusterConfig.getSlaveInstanceType())
                        .withEmrManagedMasterSecurityGroup(clusterConfig.getEmrManagedMasterSecurityGroup())
                        .withEmrManagedSlaveSecurityGroup(clusterConfig.getEmrManagedSlaveSecurityGroup())
                        .withEc2SubnetId(clusterConfig.getEc2SubnetId()));
    }

}
