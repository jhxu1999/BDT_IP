package tables;

import dataStructure.baseTable;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Objects;

public class supplier extends baseTable {
    public Long s_suppkey;
    public String s_name;
    public String s_address;
    public Long n_nationkey;
    public String s_phone;
    public double s_acctbal;
    public String s_comment;

    public supplier(Long s_suppkey, String s_name, String s_address, Long n_nationkey, String s_phone, double s_acctbal, String s_comment) {
        this.s_suppkey = s_suppkey;
        this.s_name = s_name;
        this.s_address = s_address;
        this.n_nationkey = n_nationkey;
        this.s_phone = s_phone;
        this.s_acctbal = s_acctbal;
        this.s_comment = s_comment;
        assertionKeys=new Hashtable<>();
        assertionKeyNames=new ArrayList<>();
    }

    public supplier(){
    }

    @Override
    public Long getPK() {
        return s_suppkey;
    }

    @Override
    public Long getKey(String keyName) {
        if(Objects.equals(keyName, "s_suppkey")) return s_suppkey;
        if(Objects.equals(keyName, "n_nationkey")) return n_nationkey;
        else throw new RuntimeException("No "+keyName+" getKey function!");
    }
    public ArrayList<String> assertionKeyNames;
    public Hashtable<String, Long> assertionKeys;

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
