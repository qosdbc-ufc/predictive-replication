package qosdbc.allocation;

import java.util.ArrayList;
import java.util.Collection;

public class VM {

	private String name;
	private Collection<Tenant> tenants;
	
	public VM(String name) {
		super();
		this.name = name;
		tenants = new ArrayList<Tenant>();
	}

	public VM(String name, Collection<Tenant> tenants) {
		super();
		this.name = name;
		this.tenants = tenants;
	}
	
	public void addTenant(Tenant t){
		tenants.add(t);
	}
	
	public double computeMeanWithTenant(Tenant t){
		Double total = 0.;
		
		for(Tenant te : tenants) total += te.computeMean();
		total += t.computeMean();
			
		return total/tenants.size();
	}
	

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Collection<Tenant> getTenants() {
		return tenants;
	}

	public void setTenants(Collection<Tenant> tenants) {
		this.tenants = tenants;
	}
	
	

}
