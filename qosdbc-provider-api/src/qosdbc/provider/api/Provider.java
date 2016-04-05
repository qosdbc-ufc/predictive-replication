/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.provider.api;

import java.util.List;

/**
 *
 * @author Leonardo Oliveira Moreira
 *
 * Interface used to standardize the methods of access to the provider. There
 * must be an implementation of this interface for each provider.
 */
public interface Provider {

    /**
     * Method to connect to the provider
     *
     * @throws ProviderException
     */
    public void connect() throws ProviderException;

    /**
     * Method to retrieve the virtual machine by your identification
     *
     * @param id
     * @return
     * @throws ProviderException
     */
    public VirtualMachine getVirtualMachine(String id) throws ProviderException;

    /**
     * Method to retrieve all virtual machines of the provider
     *
     * @return
     * @throws ProviderException
     */
    public List<VirtualMachine> getVirtualMachineList() throws ProviderException;

    /**
     * Method to retrieve all stopped virtual machines of the provider
     *
     * @return
     * @throws ProviderException
     */
    public List<VirtualMachine> getStoppedVirtualMachineList() throws ProviderException;

    /**
     * Method to retrieve all running virtual machines of the provider
     *
     * @return
     * @throws ProviderException
     */
    public List<VirtualMachine> getRunningVirtualMachineList() throws ProviderException;

    /**
     * Method to retrieve all stopped virtual machines of the provider that are
     * of the owner passed by parameter
     *
     * @return
     * @throws ProviderException
     */
    public List<VirtualMachine> getStoppedVirtualMachineList(String owner) throws ProviderException;

    /**
     * Method to retrieve all started virtual machines of the provider that are
     * of the owner passed by parameter
     *
     * @return
     * @throws ProviderException
     */
    public List<VirtualMachine> getRunningVirtualMachineList(String owner) throws ProviderException;

    /**
     * Method to retrieve all of the virtual machines of the provider that are
     * of owner informed by parameter
     *
     * @param owner
     * @return
     * @throws ProviderException
     */
    public List<VirtualMachine> getVirtualMachineList(String owner) throws ProviderException;

    /**
     * Method to start a virtual machine (VM) by the virtual machine name object
     *
     * @param virtualMachine
     * @throws ProviderException
     */
    public void startVirtualMachine(VirtualMachine virtualMachine) throws ProviderException;

    /**
     * Method to stop a virtual machine (VM) by the virtual machine object
     *
     * @param virtualMachine
     * @throws ProviderException
     */
    public void stopVirtualMachine(VirtualMachine virtualMachine) throws ProviderException;

    /**
     * Method to disconnect to the provider
     *
     * @throws ProviderException
     */
    public void disconnect() throws ProviderException;
}