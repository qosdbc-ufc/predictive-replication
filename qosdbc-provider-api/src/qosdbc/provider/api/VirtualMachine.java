/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.provider.api;

/**
 *
 * @author Leonardo Oliveira Moreira
 *
 * Class to represent a virtual machine (VM) of this API
 */
public class VirtualMachine {

    private String id = "UNKNOWN";
    private String owner = "UNKNOWN";
    private String privateHost = "UNKNOWN";
    private String publicHost = "UNKNOWN";
    private String privateDNSHost = "UNKNOWN";
    private String publicDNSHost = "UNKNOWN";
    private Integer stateCode;

    public String getPrivateDNSHost() {
        return privateDNSHost;
    }

    public void setPrivateDNSHost(String privateDNSHost) {
        this.privateDNSHost = privateDNSHost;
    }

    public String getPublicDNSHost() {
        return publicDNSHost;
    }

    public void setPublicDNSHost(String publicDNSHost) {
        this.publicDNSHost = publicDNSHost;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getPrivateHost() {
        return privateHost;
    }

    public void setPrivateHost(String privateHost) {
        this.privateHost = privateHost;
    }

    public String getPublicHost() {
        return publicHost;
    }

    public void setPublicHost(String publicHost) {
        this.publicHost = publicHost;
    }

    public Integer getStateCode() {
        return stateCode;
    }

    public void setStateCode(Integer stateCode) {
        this.stateCode = stateCode;
    }

    @Override
    public String toString() {
        String stateCodeStr = "UNKNOWN";
        switch (stateCode) {
            case VirtualMachineState.RUNNING: {
                stateCodeStr = "RUNNING";
                break;
            }
            case VirtualMachineState.STOPPED: {
                stateCodeStr = "STOPPED";
                break;
            }
        }
        String string = "";
        string += "ID: " + (id == null || id.trim().length() == 0 ? "UNKNOWN" : id);
        string += ", Owner: " + (owner == null || owner.trim().length() == 0 ? "UNKNOWN" : owner);
        string += ", PublicHost: " + (publicHost == null || publicHost.trim().length() == 0 ? "UNKNOWN" : publicHost);
        string += ", PublicDNSHost: " + (publicDNSHost == null || publicDNSHost.trim().length() == 0 ? "UNKNOWN" : publicDNSHost);
        string += ", PrivateHost: " + (privateHost == null || privateHost.trim().length() == 0 ? "UNKNOWN" : privateHost);
        string += ", PrivateDNSHost: " + (privateDNSHost == null || privateDNSHost.trim().length() == 0 ? "UNKNOWN" : privateDNSHost);
        string += ", StateCode: " + (stateCodeStr);
        return string;
    }
}