package baleian.example.java.aws.emr;

import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.Step;

public interface EmrRequest {

    EmrRequest withOnBeforeStep(DescribeStepRequest stepRequest);

    EmrRequest withOnAfterStep(Step step);

    EmrRequestAsync async();

    EmrResult execute();

}
