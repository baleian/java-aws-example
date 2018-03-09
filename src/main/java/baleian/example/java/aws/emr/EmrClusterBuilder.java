package baleian.example.java.aws.emr;

import com.amazonaws.auth.AWSCredentials;
import javafx.util.Builder;

public class EmrClusterBuilder implements Builder<EmrCluster> {

    private AWSCredentials credentials;

    private String region;

    private String clusterId;

    public EmrClusterBuilder withCredentials(AWSCredentials credentials) {
        this.credentials = credentials;
        return this;
    }

    public EmrClusterBuilder withRegion(String region) {
        this.region = region;
        return this;
    }

    public EmrClusterBuilder withClusterId(String clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public EmrCluster build() {
        EmrCluster cluster = new EmrCluster();
        cluster.setCredentials(credentials);
        cluster.setRegion(region);
        cluster.setClusterId(clusterId);
        return null;
    }

}
