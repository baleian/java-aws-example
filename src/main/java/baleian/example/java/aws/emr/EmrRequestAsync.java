package baleian.example.java.aws.emr;

import com.amazonaws.services.elasticmapreduce.model.Step;

public interface EmrRequestAsync {

    EmrRequestAsync withOnSuccess(Step step);

    EmrRequestAsync withOnFailure(Step step);

    EmrRequestAsync withOnError(EmrExecuteException e);

    String[] execute();

}
