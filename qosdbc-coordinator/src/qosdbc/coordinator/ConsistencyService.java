package qosdbc.coordinator;

import qosdbc.commons.jdbc.QoSDBCMessage;
import qosdbc.commons.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class ConsistencyService {
  private Lock lock = null;
  private HashMap<Pair<String, String>, ConcurrentLinkedQueue<QoSDBCMessage.Request>> pendingUpdatesMap = null;

  public ConsistencyService() {
    this.pendingUpdatesMap = new HashMap<Pair<String, String>, ConcurrentLinkedQueue<QoSDBCMessage.Request>>();
    this.lock = new ReentrantLock();
  }

  /**
   *
   * @param hostIp Name of db + ip
   */
  public void addTenantAtHost(String dbName, String host) {
    Pair<String, String> replica =  new Pair<String, String>(dbName, host);
    if (!pendingUpdatesMap.containsKey(replica)) {
      ConcurrentLinkedQueue<QoSDBCMessage.Request> queueOfPendingUpdates = new ConcurrentLinkedQueue<QoSDBCMessage.Request>();
      pendingUpdatesMap.put(replica, queueOfPendingUpdates);
    }
  }

  public void addPendingUpdate(String dbName, String host, QoSDBCMessage.Request request) {
    Iterator it = pendingUpdatesMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry)it.next();
      Pair<String, String> key = (Pair<String, String>)pair.getKey();
      if (key.getLeft().equals(dbName) && !key.getRight().equals(host)) {
        ConcurrentLinkedQueue<QoSDBCMessage.Request> queue = (ConcurrentLinkedQueue<QoSDBCMessage.Request>)pair.getValue();
        queue.add(request);
      }
    }
  }

  public ArrayList<QoSDBCMessage.Request> getPendingRequestFor(String dbName, String host, long time) {
    Pair<String, String> replica =  new Pair<String, String>(dbName, host);
    if (pendingUpdatesMap.containsKey(replica)) {
      return selectPendingRequests(pendingUpdatesMap.get(replica), time);
    }
    return null;
  }

  public void clearPendingRequestsFor(String dbName, String host) {
    Pair<String, String> replica =  new Pair<String, String>(dbName, host);
    if (pendingUpdatesMap.containsKey(replica)) {
      pendingUpdatesMap.get(replica).clear();
    }
  }

  public ArrayList<QoSDBCMessage.Request> selectPendingRequests(ConcurrentLinkedQueue<QoSDBCMessage.Request> queue, long time) {
    ArrayList<QoSDBCMessage.Request> ret = new ArrayList<QoSDBCMessage.Request>();
    QoSDBCMessage.Request req = queue.peek();
    while (req != null) {
      if(req.getConnectionId() <= time) {
        ret.add(queue.poll());
      } else {
        break;
      }
      req = queue.peek();
    }
    return ret;
  }
}