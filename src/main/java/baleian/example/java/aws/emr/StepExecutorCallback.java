package baleian.example.java.aws.emr;

import com.amazonaws.services.elasticmapreduce.model.Step;

public interface StepExecutorCallback {

    void onSuccee(Step step);
    void onFailure(Step step);
    void onError(Step step, Exception e);

}
