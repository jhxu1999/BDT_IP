package tables;

import dataStructure.baseTable;

import java.util.ArrayList;
import java.util.Hashtable;

public class nation extends baseTable {
    public Long n_nationkey;
    public String n_name;
    public Long r_regionkey;
    public String n_comment;

    public nation(Long n_nationkey, String n_name, Long r_regionkey, String n_comment) {
        this.n_nationkey = n_nationkey;
        this.n_name = n_name;
        this.r_regionkey = r_regionkey;
        this.n_comment = n_comment;
        assertionKeys=new Hashtable<>();
        assertionKeyNames=new ArrayList<>();
    }

    public nation() {
    }


    @Override
    public Long getPK() {
        return n_nationkey;
    }

    @Override
    public Long getKey(String keyName) {
        if(keyName=="r_regionkey") return r_regionkey;
        else throw new RuntimeException("No "+keyName+" getKey function!");
    }

    public ArrayList<String> assertionKeyNames=new ArrayList<>();
    public Hashtable<String, Long> assertionKeys=new Hashtable<>();

    @Override
    public ArrayList<String> getAssertionKeyNames() {
        return assertionKeyNames;
    }
    @Override
    public Long getAssertionKeyValues(String assertionKeyName) {
        return assertionKeys.get(assertionKeyName);
    }
    @Override
    public void setAssertionKeys(String assertionKeyName, Long assertionKeyValue) {
        assertionKeys.put(assertionKeyName,assertionKeyValue);
    }
}
