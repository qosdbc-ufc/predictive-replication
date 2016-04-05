/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.provider.api;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesResult;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesResult;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Leonardo Oliveira Moreira
 *
 * Class that implements the access to the Amazon provider
 */
public class ProviderAmazonImpl implements Provider {

    private static final int WAIT_FOR_TRANSITION_INTERVAL = 5000;
    private AmazonEC2 amazonEC2 = null;
    private boolean connected = false;
    
    public ProviderAmazonImpl() {
    }

    @Override
    public void connect() throws ProviderException {
        try {
            InputStream credentialsAsStream = Main.class.getResourceAsStream("AwsCredentials.properties");
            AWSCredentials credentials = new PropertiesCredentials(credentialsAsStream);
            amazonEC2 = new AmazonEC2Client(credentials);
            amazonEC2.setEndpoint("ec2.us-east-1.amazonaws.com");
            connected = true;
        } catch (IOException ex) {
            connected = false;
            throw new ProviderException(ex);
        }
    }

    /**
     * Method used to get a Amazon Instance by the instanceId
     *
     * @param instanceId
     * @return
     */
    private Instance getInstance(String instanceId) {
        DescribeInstancesResult describeInstancesResult = amazonEC2.describeInstances();
        List<Reservation> reservations = describeInstancesResult.getReservations();
        for (int i = 0; reservations != null && i < reservations.size(); i++) {
            Reservation reservation = reservations.get(i);
            List<Instance> instances = reservation.getInstances();
            for (int j = 0; instances != null && j < instances.size(); j++) {
                Instance instance = instances.get(j);
                if (instance.getInstanceId().equalsIgnoreCase(instanceId)) {
                    return instance;
                }
            }
        }
        return null;
    }

    @Override
    public List<VirtualMachine> getVirtualMachineList() throws ProviderException {
        if (!connected) {
            throw new ProviderException("There is not an established connection with the provider");
        }
        List<VirtualMachine> result = new ArrayList<VirtualMachine>();
        DescribeInstancesResult describeInstancesResult = amazonEC2.describeInstances();
        List<Reservation> reservations = describeInstancesResult.getReservations();
        for (int i = 0; reservations != null && i < reservations.size(); i++) {
            Reservation reservation = reservations.get(i);
            List<Instance> instances = reservation.getInstances();
            for (int j = 0; instances != null && j < instances.size(); j++) {
                Instance instance = instances.get(j);
                VirtualMachine virtualMachine = new VirtualMachine();
                virtualMachine.setId(instance.getInstanceId());
                virtualMachine.setOwner(instance.getKeyName());
                if (instance.getState().getName().equals("running")) {
                    virtualMachine.setStateCode(VirtualMachineState.RUNNING);
                }
                if (instance.getState().getName().equals("stopped")) {
                    virtualMachine.setStateCode(VirtualMachineState.STOPPED);
                }
                virtualMachine.setPrivateHost(instance.getPrivateIpAddress());
                virtualMachine.setPublicHost(instance.getPublicIpAddress());
                virtualMachine.setPrivateDNSHost(instance.getPrivateDnsName());
                virtualMachine.setPublicDNSHost(instance.getPublicDnsName());
                result.add(virtualMachine);
            }
        }
        return result;
    }

    @Override
    public List<VirtualMachine> getVirtualMachineList(String owner) throws ProviderException {
        if (!connected) {
            throw new ProviderException("There is not an established connection with the provider");
        }
        List<VirtualMachine> result = new ArrayList<VirtualMachine>();
        DescribeInstancesResult describeInstancesResult = amazonEC2.describeInstances();
        List<Reservation> reservations = describeInstancesResult.getReservations();
        for (int i = 0; reservations != null && i < reservations.size(); i++) {
            Reservation reservation = reservations.get(i);
            List<Instance> instances = reservation.getInstances();
            for (int j = 0; instances != null && j < instances.size(); j++) {
                Instance instance = instances.get(j);
                if (!instance.getKeyName().equalsIgnoreCase(owner)) {
                    continue;
                }
                VirtualMachine virtualMachine = new VirtualMachine();
                virtualMachine.setId(instance.getInstanceId());
                virtualMachine.setOwner(instance.getKeyName());
                if (instance.getState().getName().equals("running")) {
                    virtualMachine.setStateCode(VirtualMachineState.RUNNING);
                }
                if (instance.getState().getName().equals("stopped")) {
                    virtualMachine.setStateCode(VirtualMachineState.STOPPED);
                }
                virtualMachine.setPrivateHost(instance.getPrivateIpAddress());
                virtualMachine.setPublicHost(instance.getPublicIpAddress());
                virtualMachine.setPrivateDNSHost(instance.getPrivateDnsName());
                virtualMachine.setPublicDNSHost(instance.getPublicDnsName());
                result.add(virtualMachine);
            }
        }
        return result;
    }

    @Override
    public void disconnect() throws ProviderException {
        amazonEC2 = null;
        connected = false;
    }

    @Override
    public void startVirtualMachine(VirtualMachine virtualMachine) throws ProviderException {
        if (!connected) {
            throw new ProviderException("There is not an established connection with the provider");
        }
        StartInstancesRequest startRequest = new StartInstancesRequest().withInstanceIds(virtualMachine.getId());
        StartInstancesResult startResult = amazonEC2.startInstances(startRequest);
        List<InstanceStateChange> stateChangeList = startResult.getStartingInstances();
        System.out.println("Starting instance '" + virtualMachine + "'");
        try {
            // Wait for the instance to be started
            System.out.println(waitForTransitionCompletion(stateChangeList, "running", virtualMachine.getId()));
        } catch (InterruptedException ex) {
            throw new ProviderException(ex);
        }
        System.out.println("Started instance '" + virtualMachine + "'");
    }

    @Override
    public void stopVirtualMachine(VirtualMachine virtualMachine) throws ProviderException {
        if (!connected) {
            throw new ProviderException("There is not an established connection with the provider");
        }
        StopInstancesRequest stopRequest = new StopInstancesRequest().withInstanceIds(virtualMachine.getId()).withForce(true);
        StopInstancesResult startResult = amazonEC2.stopInstances(stopRequest);
        List<InstanceStateChange> stateChangeList = startResult.getStoppingInstances();
        System.out.println("Stopping instance '" + virtualMachine + "'");
        try {
            // Wait for the instance to be stopped
            System.out.println(waitForTransitionCompletion(stateChangeList, "stopped", virtualMachine.getId()));
        } catch (InterruptedException ex) {
            throw new ProviderException(ex);
        }
        System.out.println("Stopped instance '" + virtualMachine + "'");
    }

    /**
     * Wait for a instance to complete transitioning (i.e. status not being in
     * INSTANCE_STATE_IN_PROGRESS_SET or the instance no longer existing).
     *
     * @param stateChangeList
     * @param desiredState
     * @param instanceId
     * @throws InterruptedException
     * @throws Exception
     */
    public final String waitForTransitionCompletion(List<InstanceStateChange> stateChangeList, final String desiredState, String instanceId) throws InterruptedException {
        Boolean transitionCompleted = false;
        InstanceStateChange stateChange = stateChangeList.get(0);
        String previousState = stateChange.getPreviousState().getName();
        String currentState = stateChange.getCurrentState().getName();
        String transitionReason = "";
        while (!transitionCompleted) {
            try {
                Instance instance = getInstance(instanceId);
                currentState = instance.getState().getName();
                if (previousState.equals(currentState)) {
                    System.out.println("'" + instanceId + "' is still in state " + currentState.toUpperCase() + " ...");
                } else {
                    System.out.println("'" + instanceId + "' entered state " + currentState.toUpperCase() + " ...");
                    transitionReason = instance.getStateTransitionReason();
                }
                previousState = currentState;
                if (currentState.equals(desiredState)) {
                    transitionCompleted = true;
                }
            } catch (AmazonServiceException ex) {
                System.out.println("Failed to describe instance '" + instanceId + "'!");
                throw ex;
            }
            // Sleep for WAIT_FOR_TRANSITION_INTERVAL seconds until transition has completed.
            if (!transitionCompleted) {
                Thread.sleep(WAIT_FOR_TRANSITION_INTERVAL);
            }
        }
        System.out.println("Transition of instance '" + instanceId + "' completed with state " + currentState.toUpperCase() + " (" + ((transitionReason.trim().length() == 0) ? "Unknown transition reason" : transitionReason.toUpperCase()) + ").");
        return currentState;
    }

    @Override
    public List<VirtualMachine> getStoppedVirtualMachineList() throws ProviderException {
        if (!connected) {
            throw new ProviderException("There is not an established connection with the provider");
        }
        List<VirtualMachine> result = new ArrayList<VirtualMachine>();
        DescribeInstancesResult describeInstancesResult = amazonEC2.describeInstances();
        List<Reservation> reservations = describeInstancesResult.getReservations();
        for (int i = 0; reservations != null && i < reservations.size(); i++) {
            Reservation reservation = reservations.get(i);
            List<Instance> instances = reservation.getInstances();
            for (int j = 0; instances != null && j < instances.size(); j++) {
                Instance instance = instances.get(j);
                if (!instance.getState().getName().equalsIgnoreCase("stopped")) {
                    continue;
                }
                VirtualMachine virtualMachine = new VirtualMachine();
                virtualMachine.setId(instance.getInstanceId());
                virtualMachine.setOwner(instance.getKeyName());
                virtualMachine.setStateCode(VirtualMachineState.STOPPED);
                virtualMachine.setPrivateHost(instance.getPrivateIpAddress());
                virtualMachine.setPublicHost(instance.getPublicIpAddress());
                virtualMachine.setPrivateDNSHost(instance.getPrivateDnsName());
                virtualMachine.setPublicDNSHost(instance.getPublicDnsName());
                result.add(virtualMachine);
            }
        }
        return result;
    }

    @Override
    public List<VirtualMachine> getRunningVirtualMachineList() throws ProviderException {
        if (!connected) {
            throw new ProviderException("There is not an established connection with the provider");
        }
        List<VirtualMachine> result = new ArrayList<VirtualMachine>();
        DescribeInstancesResult describeInstancesResult = amazonEC2.describeInstances();
        List<Reservation> reservations = describeInstancesResult.getReservations();
        for (int i = 0; reservations != null && i < reservations.size(); i++) {
            Reservation reservation = reservations.get(i);
            List<Instance> instances = reservation.getInstances();
            for (int j = 0; instances != null && j < instances.size(); j++) {
                Instance instance = instances.get(j);
                if (!instance.getState().getName().equalsIgnoreCase("running")) {
                    continue;
                }
                VirtualMachine virtualMachine = new VirtualMachine();
                virtualMachine.setId(instance.getInstanceId());
                virtualMachine.setOwner(instance.getKeyName());
                virtualMachine.setStateCode(VirtualMachineState.RUNNING);
                virtualMachine.setPrivateHost(instance.getPrivateIpAddress());
                virtualMachine.setPublicHost(instance.getPublicIpAddress());
                virtualMachine.setPrivateDNSHost(instance.getPrivateDnsName());
                virtualMachine.setPublicDNSHost(instance.getPublicDnsName());
                result.add(virtualMachine);
            }
        }
        return result;
    }

    @Override
    public List<VirtualMachine> getStoppedVirtualMachineList(String owner) throws ProviderException {
        if (!connected) {
            throw new ProviderException("There is not an established connection with the provider");
        }
        List<VirtualMachine> result = new ArrayList<VirtualMachine>();
        DescribeInstancesResult describeInstancesResult = amazonEC2.describeInstances();
        List<Reservation> reservations = describeInstancesResult.getReservations();
        for (int i = 0; reservations != null && i < reservations.size(); i++) {
            Reservation reservation = reservations.get(i);
            List<Instance> instances = reservation.getInstances();
            for (int j = 0; instances != null && j < instances.size(); j++) {
                Instance instance = instances.get(j);
                if (!instance.getState().getName().equalsIgnoreCase("stopped") && !instance.getKeyName().equals(owner)) {
                    continue;
                }
                VirtualMachine virtualMachine = new VirtualMachine();
                virtualMachine.setId(instance.getInstanceId());
                virtualMachine.setOwner(instance.getKeyName());
                virtualMachine.setStateCode(VirtualMachineState.STOPPED);
                virtualMachine.setPrivateHost(instance.getPrivateIpAddress());
                virtualMachine.setPublicHost(instance.getPublicIpAddress());
                virtualMachine.setPrivateDNSHost(instance.getPrivateDnsName());
                virtualMachine.setPublicDNSHost(instance.getPublicDnsName());
                result.add(virtualMachine);
            }
        }
        return result;
    }

    @Override
    public List<VirtualMachine> getRunningVirtualMachineList(String owner) throws ProviderException {
        if (!connected) {
            throw new ProviderException("There is not an established connection with the provider");
        }
        List<VirtualMachine> result = new ArrayList<VirtualMachine>();
        DescribeInstancesResult describeInstancesResult = amazonEC2.describeInstances();
        List<Reservation> reservations = describeInstancesResult.getReservations();
        for (int i = 0; reservations != null && i < reservations.size(); i++) {
            Reservation reservation = reservations.get(i);
            List<Instance> instances = reservation.getInstances();
            for (int j = 0; instances != null && j < instances.size(); j++) {
                Instance instance = instances.get(j);
                if (!instance.getState().getName().equalsIgnoreCase("running") && !instance.getKeyName().equals(owner)) {
                    continue;
                }
                VirtualMachine virtualMachine = new VirtualMachine();
                virtualMachine.setId(instance.getInstanceId());
                virtualMachine.setOwner(instance.getKeyName());
                virtualMachine.setStateCode(VirtualMachineState.RUNNING);
                virtualMachine.setPrivateHost(instance.getPrivateIpAddress());
                virtualMachine.setPublicHost(instance.getPublicIpAddress());
                virtualMachine.setPrivateDNSHost(instance.getPrivateDnsName());
                virtualMachine.setPublicDNSHost(instance.getPublicDnsName());
                result.add(virtualMachine);
            }
        }
        return result;
    }

    @Override
    public VirtualMachine getVirtualMachine(String id) throws ProviderException {
        if (!connected) {
            throw new ProviderException("There is not an established connection with the provider");
        }
        Instance instance = this.getInstance(id);
        if (instance != null) {
            VirtualMachine virtualMachine = new VirtualMachine();
            virtualMachine.setId(instance.getInstanceId());
            virtualMachine.setOwner(instance.getKeyName());
            virtualMachine.setStateCode(VirtualMachineState.RUNNING);
            virtualMachine.setPrivateHost(instance.getPrivateIpAddress());
            virtualMachine.setPublicHost(instance.getPublicIpAddress());
            virtualMachine.setPrivateDNSHost(instance.getPrivateDnsName());
            virtualMachine.setPublicDNSHost(instance.getPublicDnsName());
            return virtualMachine;
        } else {
            throw new ProviderException("Could not find a virtual machine with this identification");
        }
    }
}