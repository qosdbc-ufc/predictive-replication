package qosdbc.coordinator;

import qosdbc.commons.OutputMessage;
import qosdbc.commons.PendingRequest;
import qosdbc.commons.jdbc.QoSDBCMessage;
import qosdbc.commons.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class ConsistencyService {
  private Lock lock = null;
  private HashMap<Pair<String, String>, ConcurrentLinkedQueue<PendingRequest>> pendingUpdatesMap = null;
  private HashMap<Pair<String, String>, Lock> lockMap = null;

  public ConsistencyService() {
    this.pendingUpdatesMap = new HashMap<Pair<String, String>, ConcurrentLinkedQueue<PendingRequest>>();
    lockMap = new HashMap<Pair<String, String>, Lock>();
    this.lock = new ReentrantLock(true);
  }

  /**
   *
   * @param hostIp Name of db + ip
   */
  public void addTenantAtHost(String dbName, String host) {
    Pair<String, String> replica =  new Pair<String, String>(dbName, host);
    if (!pendingUpdatesMap.containsKey(replica)) {
     // OutputMessage.println("[addTenantAtHost]: " + dbName + " << " + host);
      ConcurrentLinkedQueue<PendingRequest> queueOfPendingUpdates = new ConcurrentLinkedQueue<PendingRequest>();
      Lock newlock = new ReentrantLock(true);
      lock.lock();
        pendingUpdatesMap.put(replica, queueOfPendingUpdates);
        lockMap.put(replica, newlock);
      lock.unlock();
    }
  }

  public void addPendingUpdate(String dbName, String host, QoSDBCMessage.Request request) {
    lock.lock();
      Iterator it = pendingUpdatesMap.entrySet().iterator();
    lock.unlock();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry)it.next();
      Pair<String, String> key = (Pair<String, String>)pair.getKey();
      if (key.getLeft().equals(dbName) && !key.getRight().equals(host)) {
        PendingRequest pendingRequest = new PendingRequest(request.getTransactionId(), request.getCommand());
        ConcurrentLinkedQueue<PendingRequest> queue = (ConcurrentLinkedQueue<PendingRequest>)pair.getValue();
        //OutputMessage.println("[addPendingUpdate] from: " + dbName + " << " + host + " << " + key.getRight());
        lockMap.get(key).lock();
          queue.add(pendingRequest);
        lockMap.get(key).unlock();
      }
    }
  }

  public ArrayList<PendingRequest> getPendingRequestFor(String dbName, String host, long time) {
    Pair<String, String> replica =  new Pair<String, String>(dbName, host);
    /*OutputMessage.println("Requestting for " + dbName + " << " + host);
    OutputMessage.println("All: ");
    Iterator it = pendingUpdatesMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry)it.next();
      Pair<String, String> key = (Pair<String, String>)pair.getKey();
      OutputMessage.println(key.getLeft() + " << " + key.getRight());
    }*/

    lock.lock();
      if (pendingUpdatesMap.containsKey(replica)) {
        //OutputMessage.println("[getPendingRequestFor]: " + dbName + " << " + host);
        ConcurrentLinkedQueue<PendingRequest> queue = pendingUpdatesMap.get(replica);
        lockMap.get(replica).lock();
          ArrayList<PendingRequest> list = selectPendingRequests(queue, time);
        lockMap.get(replica).unlock();
        lock.unlock();
        return list;
      }
    lock.unlock();
    return null;
  }

  public void clearPendingRequestsFor(String dbName, String host) {
    Pair<String, String> replica =  new Pair<String, String>(dbName, host);
    if (pendingUpdatesMap.containsKey(replica)) {
      pendingUpdatesMap.get(replica).clear();
    }
  }

  public ArrayList<PendingRequest> selectPendingRequests(ConcurrentLinkedQueue<PendingRequest> queue, long time) {
    ArrayList<PendingRequest> ret = new ArrayList<PendingRequest>();
    PendingRequest req = queue.peek();
    while (req != null) {
      if(req.getTransactionId() < time) {
        ret.add(queue.poll());
      } else {
        break;
      }
      req = queue.peek();
    }
    return ret;
  }
}