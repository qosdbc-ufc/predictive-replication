/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.provider.api;

import java.util.List;

/**
 *
 * @author leoomoreira
 */
public class Main {

    public static void main(String[] args) {
        try {
            Provider provider = new ProviderAmazonImpl();
            provider.connect();
            VirtualMachine target = null;
            List<VirtualMachine> virtualMachines = null;
            virtualMachines = provider.getVirtualMachineList("leonardo");
            System.out.println("Show all Virtual Machine by owner 'leonado'");
            for (VirtualMachine vm : virtualMachines) {
                System.out.println(vm);
                target = vm;
            }
            System.out.println("Target VM: " + target);
            /*
            System.out.println("Start the target VM");
            provider.startVirtualMachine(target);
            virtualMachines = provider.getVirtualMachineList("leonardo");            
            for (VirtualMachine vm : virtualMachines) {
                System.out.println(vm);
            }
            System.out.println("Stopped the target VM");
            provider.stopVirtualMachine(target);
            virtualMachines = provider.getVirtualMachineList("leonardo");
            for (VirtualMachine vm : virtualMachines) {
                System.out.println(vm);
            }
            */ 
            provider.disconnect();
        } catch (ProviderException ex) {
            ex.printStackTrace();
        }
    }
}
