package qosdbc.coordinator;

import qosdbc.commons.jdbc.QoSDBCMessage;
import qosdbc.commons.Pair;
import java.util.Iterator;
import java.util.Map;

import java.util.HashMap;
import java.util.LinkedList;

class ConsistencyService {
  private HashMap<Pair<String, String>, ConcurrentLinkedQueue<QoSDBCMessage.Request>> pendingUpdatesMap = null;

  public ConsistencyService() {
    this.pendingUpdatesMap = new HashMap<Pair<String, String>, ConcurrentLinkedQueue<QoSDBCMessage.Request>>();
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

  public ConcurrentLinkedQueue<QoSDBCMessage.Request> getPendingRequestFor(String dbName, String host) {
    Pair<String, String> replica =  new Pair<String, String>(dbName, host);
    if (pendingUpdatesMap.containsKey(replica)) {
      return pendingUpdatesMap.get(replica);
    }
    return null;
  }

  public void clearPendingRequestsFor(String dbName, String host) {
    Pair<String, String> replica =  new Pair<String, String>(dbName, host);
    if (pendingUpdatesMap.containsKey(replica)) {
      pendingUpdatesMap.get(replica).clear();
    }
  }
}