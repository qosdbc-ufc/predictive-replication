package qosdbc.allocation;

import java.util.ArrayList;
import java.util.Collection;

public class Tenant {
	
	private String name;
	private Collection<Long> responseTimes;
	
		
	
	public Tenant(String name) {
		super();
		this.name = name;
		responseTimes = new ArrayList<Long>();
	}

	public Tenant(String name, Collection<Long> responseTimes) {
		super();
		this.name = name;
		this.responseTimes = responseTimes;
	}	
	
	
	public Double computeMean(){
		
		Long total = 0L;
		
		for(Long l : responseTimes) total += l;
			
		return (double) (total/responseTimes.size());
		
	}
	
	public void addResponseTime(Long l){
		responseTimes.add(l);
	}
	

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Collection<Long> getResponseTimes() {
		return responseTimes;
	}

	public void setResponseTimes(Collection<Long> responseTimes) {
		this.responseTimes = responseTimes;
	}
}
