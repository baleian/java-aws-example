package baleian.example.java.aws.emr;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

import java.util.Arrays;
import java.util.List;

public class StepExecutorTest {

    public static void main(String[] args) {
        AWSCredentials credentials = new BasicAWSCredentials("<AWS_ACCESS_KEY_ID>", "<AWS_SECRET_ACCESS_KEY>");

        AmazonElasticMapReduce client = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion("<AWS_REGION>")
                .build();

        // A custom step
        HadoopJarStepConfig hadoopJarStepConfig = new HadoopJarStepConfig()
                .withJar("s3://<BUCKET_NAME>/<JAR_PATH>")
                .withMainClass("<MAIN_CLASS_PATH>") // optional main class, this can be omitted if jar above has a manifest
                .withArgs("<ARG1>", "<ARG2>..."); // optional list of arguments
        StepConfig hadoopStepConfig = new StepConfig("<HADOOP_STEP_NAME>", hadoopJarStepConfig).withActionOnFailure(ActionOnFailure.CONTINUE);

        StepConfig hiveStepConfig = new StepConfig("<HIVE_STEP_NAME>", new StepFactory("<AWS_REGION>.elasticmapreduce")
                .newRunHiveScriptStep("s3://<SCRIPT_PATH>")
                .withProperties(new KeyValue("<PROPERTY_KEY1>", "<PROPERTY_VALUE1>"), new KeyValue("<PROPERTY_KEY2>", "<PROPERTY_VALUE2>"))
        ).withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW);


        // if you want to run at temporary cluster
//        ClusterConfig clusterConfig = new ClusterConfig();
//        clusterConfig.setClusterName("simple-job");
//        clusterConfig.setJobFlowRole("EMR_EC2_DefaultRole");
//        clusterConfig.setServiceRole("EMR_DefaultRole");
//        clusterConfig.setAutoScalingRole("EMR_AutoScaling_DefaultRole");
//        clusterConfig.setReleaseLabel("emr-5.10.0");
//        clusterConfig.setLogUri("s3://baleian-emr/logs/");
//        clusterConfig.setApplications(new Application().withName("Hive"));
//        clusterConfig.setVisibleToAllUsers(true);
//        clusterConfig.setHadoopVersion("Amazon 2.7.3");
//        clusterConfig.setInstanceCount(3);
//        clusterConfig.setMasterInstanceType("m4.large");
//        clusterConfig.setSlaveInstanceType("m4.large");
//        clusterConfig.setEmrManagedMasterSecurityGroup("sg-3152f15a");
//        clusterConfig.setEmrManagedSlaveSecurityGroup("sg-0a53f061");
//        clusterConfig.setEc2SubnetId("subnet-0610b56e");

        // if you already have running cluster
        String clusterId = "<CLUSTER_ID>";

        StepExecutor stepExecutor = new StepExecutor();
        stepExecutor.setClient(client);

//        List<Step> steps = stepExecutor.execute(clusterConfig, Arrays.asList(new StepConfig[]{hadoopStepConfig, hiveStepConfig}), new StepExecutorCallback() {
        List<Step> steps = stepExecutor.execute(clusterId, Arrays.asList(new StepConfig[]{hadoopStepConfig, hiveStepConfig}), new StepExecutorCallback() {
            public void onSuccee(Step step) {
                System.out.println("onSuccess: " + step);
            }

            public void onFailure(Step step) {
                System.out.println("onFailure: " + step);
            }

            public void onError(Step step, Exception e) {
                System.out.println("onError: " + step);
                e.printStackTrace();
            }
        });

        for (Step step : steps) {
            System.out.println(step);
        }

    }

}
