package dataStructure;

import java.util.Hashtable;
import java.util.concurrent.CopyOnWriteArrayList;

public class indexChild {
    public String relation;
    public String childName;
    public Hashtable<Long, CopyOnWriteArrayList<Long>> index;

    public indexChild(String baseRelationName, String childRelationName) {
        this.relation = baseRelationName;
        this.childName = childRelationName;
        this.index = new Hashtable<Long, CopyOnWriteArrayList<Long>>();
    }

}
