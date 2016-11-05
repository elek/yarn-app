package net.anzix.yarn;

import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;

import java.util.List;

public class Worker {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("HELO WORLD START");
        List<VirtualMachineDescriptor> vms = VirtualMachine.list();
        for (VirtualMachineDescriptor vmd : vms) {
            System.out.println(vmd.toString());
        }
        Thread.sleep(600000);
        System.out.println("HELO WORLD END");
    }
}
