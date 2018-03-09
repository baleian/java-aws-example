package baleian.example.java.aws.emr;

import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public interface EmrCluster {

    String create(RunJobFlowResult);

    boolean terminate();

    EmrRequest request(StepConfig... stepConfigs);

}
