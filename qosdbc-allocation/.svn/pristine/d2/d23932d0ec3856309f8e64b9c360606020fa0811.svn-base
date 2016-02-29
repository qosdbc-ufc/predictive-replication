package qosdbc.allocation;

import java.util.ArrayList;
import java.util.Collection;

/**
 * 
 * @author Victor Farias
 */
public class DB {

    private String name;
    private Collection<Long> responseTimes;

    public DB(String name) {
        super();
        this.name = name;
        responseTimes = new ArrayList<Long>();
    }

    public DB(String name, Collection<Long> responseTimes) {
        super();
        this.name = name;
        this.responseTimes = responseTimes;
    }

    public Double computeMean() {

        Long total = 0L;

        for (Long l : responseTimes) {
            total += l;
        }

        return (double) (total / responseTimes.size());

    }

    public void addResponseTime(Long l) {
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