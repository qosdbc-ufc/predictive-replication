package qosdbc.allocation;

import java.awt.dnd.DnDConstants;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class Allocate {

	private String migrateVMName, migrateDBName;
	HashMap<String, VM> map;
	Tenant migrateTenant;
	VM migrateVm;


	public Allocate(String migrateVM, String migrateDB) {
		super();
		this.migrateVMName = migrateVM;
		this.migrateDBName = migrateDB;
		map = new HashMap<String, VM>();
		
	}

	public VM alocate(){


		double min = Double.MAX_VALUE;
		VM vm = null;

		getDbmsAndTenants();

		Collection<VM> vms = map.values();

		for(VM v : vms){
			double aux = v.computeMeanWithTenant(migrateTenant);
			if(aux < min){
				min = aux;
				vm = v;
			}
		}
		return vm;
	}

	private void getDbmsAndTenants(){
		Connection connection = new DBConnection().connect("ec2-54-226-109-38.compute-1.amazonaws.com", 
				"qosdbc-catalog", "5432", "postgres", "ufc123");
//		Connection connection = new DBConnection().connect("127.0.0.1", 
//				"postgres", "5432", "postgres", "postgres");
		Statement st = null;
		ResultSet rs = null;
		String vmName = null, dbName = null;

		VM vm = null;

		try {

			st = connection.createStatement();
			//rs = st.executeQuery("SELECT vm_id, db_name FROM db_active");
			rs = st.executeQuery("SELECT DISTINCT vm_id, db_name FROM sql_log");
			Collection<Long> times;
			

			while (rs.next()) {
//				vmName = rs.getString(1);
//				dbName = rs.getString(2);
				
				vmName = rs.getString("vm_id");
				dbName = rs.getString("db_name");
				
				System.out.println("vm_id "+vmName);
				System.out.println("db_name "+dbName);
				if(map.containsKey(vmName)){
					vm = map.get(vmName);
				}else{
					vm = new VM(vmName);
					map.put(vmName, vm);
				}

				times = getTimes(dbName, vmName);

				Tenant t = new Tenant(dbName,times);
				vm.addTenant(t);

				System.out.println("migrate db name: "+migrateDBName);
				System.out.println("migrate vm name: "+migrateVMName);

				if(dbName.equals(migrateDBName) && vmName.equals(migrateVMName)){
					migrateTenant = t;
					migrateVm = vm;
				}else{
//					System.out.println("Nome errado fornecido!");
//					return;
				}

			}
		} catch (SQLException ex) {
			ex.printStackTrace();

		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (st != null) {
					st.close();
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}		
	}

	private Collection<Long> getTimes(String dbName, String vmName) { 
		Statement st = null;
		ResultSet rs = null;
		Connection connection = new DBConnection().connect("ec2-54-226-109-38.compute-1.amazonaws.com", 
				"qosdbc-log", "5432", "postgres", "ufc123");
//		Connection connection = new DBConnection().connect("127.0.0.1", 
//				"postgres", "5432", "postgres", "postgres");
		Collection<Long> times = new ArrayList<Long>(); 

		try {
			
			String sql = "SELECT response_time FROM sql_log WHERE vm_id = '"+vmName+"' AND db_name = '"+dbName+"' ORDER BY time DESC LIMIT 300";
			System.out.println(sql);
			
			st = connection.createStatement();
			rs = st.executeQuery(sql);
			while(rs.next())
				times.add(rs.getLong(1));
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		return times;
	}
}
