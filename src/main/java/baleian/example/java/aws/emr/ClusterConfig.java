package baleian.example.java.aws.emr;

import com.amazonaws.services.elasticmapreduce.model.Application;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

class ClusterConfig {

    private String clusterName;

    private String jobFlowRole;

    private String serviceRole;

    private String autoScalingRole;

    private String releaseLabel;

    private String logUri;

    private List<Application> applications;

    private boolean visibleToAllUsers;

    private String hadoopVersion;

    private int instanceCount;

    private String masterInstanceType;

    private String slaveInstanceType;

    private String emrManagedMasterSecurityGroup;

    private String emrManagedSlaveSecurityGroup;

    private String ec2SubnetId;


    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getJobFlowRole() {
        return jobFlowRole;
    }

    public void setJobFlowRole(String jobFlowRole) {
        this.jobFlowRole = jobFlowRole;
    }

    public String getServiceRole() {
        return serviceRole;
    }

    public void setServiceRole(String serviceRole) {
        this.serviceRole = serviceRole;
    }

    public String getAutoScalingRole() {
        return autoScalingRole;
    }

    public void setAutoScalingRole(String autoScalingRole) {
        this.autoScalingRole = autoScalingRole;
    }

    public String getReleaseLabel() {
        return releaseLabel;
    }

    public void setReleaseLabel(String releaseLabel) {
        this.releaseLabel = releaseLabel;
    }

    public String getLogUri() {
        return logUri;
    }

    public void setLogUri(String logUri) {
        this.logUri = logUri;
    }

    public List<Application> getApplications() {
        return applications;
    }

    public void setApplications(Collection<Application> applications) {
        this.applications = Arrays.asList((Application[]) applications.toArray());
    }

    public void setApplications(Application... applications) {
        this.applications = Arrays.asList(applications);
    }

    public void addApplications(Collection<Application> applications) {
        this.applications.addAll(applications);
    }

    public void addApplications(Application... applications) {  this.applications.addAll(Arrays.asList(applications)); }

    public boolean isVisibleToAllUsers() {
        return visibleToAllUsers;
    }

    public void setVisibleToAllUsers(boolean visibleToAllUsers) {
        this.visibleToAllUsers = visibleToAllUsers;
    }

    public String getHadoopVersion() {
        return hadoopVersion;
    }

    public void setHadoopVersion(String hadoopVersion) {
        this.hadoopVersion = hadoopVersion;
    }

    public int getInstanceCount() {
        return instanceCount;
    }

    public void setInstanceCount(int instanceCount) {
        this.instanceCount = instanceCount;
    }

    public String getMasterInstanceType() {
        return masterInstanceType;
    }

    public void setMasterInstanceType(String masterInstanceType) {
        this.masterInstanceType = masterInstanceType;
    }

    public String getSlaveInstanceType() {
        return slaveInstanceType;
    }

    public void setSlaveInstanceType(String slaveInstanceType) {
        this.slaveInstanceType = slaveInstanceType;
    }

    public String getEmrManagedMasterSecurityGroup() {
        return emrManagedMasterSecurityGroup;
    }

    public void setEmrManagedMasterSecurityGroup(String emrManagedMasterSecurityGroup) {
        this.emrManagedMasterSecurityGroup = emrManagedMasterSecurityGroup;
    }

    public String getEmrManagedSlaveSecurityGroup() {
        return emrManagedSlaveSecurityGroup;
    }

    public void setEmrManagedSlaveSecurityGroup(String emrManagedSlaveSecurityGroup) {
        this.emrManagedSlaveSecurityGroup = emrManagedSlaveSecurityGroup;
    }

    public String getEc2SubnetId() {
        return ec2SubnetId;
    }

    public void setEc2SubnetId(String ec2SubnetId) {
        this.ec2SubnetId = ec2SubnetId;
    }

}
