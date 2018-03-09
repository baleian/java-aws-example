package baleian.example.java.aws.emr;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.util.Arrays;
import java.util.List;

public class StepExecutorTest {

    public static void main(String[] args) {

        AWSCredentials credentials = new BasicAWSCredentials("<AWS_ACCESS_KEY_ID>", "<AWS_SECRET_ACCESS_KEY>");

        EmrCluster cluster = new EmrClusterBuilder()
                .withCredentials(credentials)
                .withRegion("<AWS_REGION>")
                .withClusterId("<EMR_CLUSTER_ID>")
                .build();

//        String clusterId = cluster.create(new RunJobFlowRequest());
//        boolean isTerminating = cluster.terminate();


        CustomJarStepConfig customJarStepConfig = CustomJarStepConfig.builder()
                .withStepName("<STEP_NAME>")
                .withJarUri("<JAR_PATH_URI>")
                .withMainClass("<MAIN_CLASS_PATH")
                .withArgs("<SCRIPT_VALUE_NAME1>=<SCRIPT_VALUE1>", "<SCRIPT_VALUE_NAME2>=<SCRIPT_VALUE2>")
                .withActionOnFailure(ActionOnFailure.CONTINUE)
                .build();

        HiveStepConfig hiveStepConfig = HiveStepConfig.builder()
                .withStepName("<STEP_NAME>")
                .withScriptUri("<HIVE_SCRIPT_PATH_URI>")
                .withArgs("<SCRIPT_VALUE_NAME1>=<SCRIPT_VALUE1>", "<SCRIPT_VALUE_NAME2>=<SCRIPT_VALUE2>")
                .withActionOnFailure(ActionOnFailure.CONTINUE)
                .build();

        EmrResult result = cluster
                .request(customJarStepConfig, hiveStepConfig)
                .withOnBeforeStep(stepRequest -> {})
                .withOnAfterStep(step -> {})
                .execute();

        String[] stepIds = cluster
                .request(customJarStepConfig, hiveStepConfig)
                .withOnBeforeStep(stepRequest -> {})
                .withOnAfterStep(step -> {})
                .async()
                .withOnSuccess(result -> {})
                .withOnFailure(result -> {})
                .withOnError(e -> {})
                .execute();





        AmazonElasticMapReduce client = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion("<AWS_REGION>")
                .build();

        
        // A custom jar step
        HadoopJarStepConfig hadoopJarStepConfig = new HadoopJarStepConfig()
                .withJar("<JAR_PATH_URI>")
                .withMainClass("<MAIN_CLASS_PATH>") // optional main class, this can be omitted if jar above has a manifest
                .withArgs("<ARG1>", "<ARG2>..."); // optional list of arguments
        StepConfig hadoopStepConfig = new StepConfig("<HADOOP_STEP_NAME>", hadoopJarStepConfig).withActionOnFailure(ActionOnFailure.CONTINUE);

        
        // A hive script runner step
        HadoopJarStepConfig commandRunnerJarStepConfig = new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withArgs("hive-script", "--run-hive-script", "--args",
                        "-f", "<HIVE_SCRIPT_PATH_URI>",
                        "-d", "<SCRIPT_VALUE_NAME1>=<SCRIPT_VALUE1>",  // it will replace ${<SCRIPT_VALUE_NAME>} to <SCRIPT_VALUE> into hive script.
                        "-d", "<SCRIPT_VALUE_NAME2>=<SCRIPT_VALUE2>");
        StepConfig hiveStepConfig = new StepConfig("<HIVE_STEP_NAME>", commandRunnerJarStepConfig).withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);


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
//        clusterConfig.setEmrManagedMasterSecurityGroup("<MASTER_SECURITY_GROUP_ID>"); // sg-xxxxxxxx
//        clusterConfig.setEmrManagedSlaveSecurityGroup("<SLAVE_SECURITY_GROUP_ID>"); // sg-xxxxxxxx
//        clusterConfig.setEc2SubnetId("<EC2_SUBNET_ID>"); // subnet-xxxxxxxx

        
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
