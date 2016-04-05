/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.commons.data_structure;

import java.util.ArrayList;

/**
 * @author serafim
 * @param <T>
 */
public class ReplicasList<T> extends ArrayList<T>{
    private int nextReplicaIndex = 0;
    
    public ReplicasList () {
        super();
    }
    
    
    public T nextReplica() {
        T ret = null;
        if (size() > 0) {
            if (nextReplicaIndex == size()) {
                nextReplicaIndex = 0;
            }
            ret = get(nextReplicaIndex);
            nextReplicaIndex++;
        }
        return ret;
    }
}
