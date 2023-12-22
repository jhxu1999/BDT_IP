package tables;

import dataStructure.baseTable;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Objects;

public class customer extends baseTable {
    public Long c_custkey;
    public String c_name;
    public String c_address;
    public Long n_nationkey;
    public String c_phone;
    public double c_acctbal;
    public String c_mktsegment;
    public String c_comment;

    public customer(Long c_custkey, String c_name, String c_address, Long n_nationkey, String c_phone, double c_acctbal, String c_mktsegment, String c_comment) {
        this.c_custkey = c_custkey;
        this.c_name = c_name;
        this.c_address = c_address;
        this.n_nationkey = n_nationkey;
        this.c_phone = c_phone;
        this.c_acctbal = c_acctbal;
        this.c_mktsegment = c_mktsegment;
        this.c_comment = c_comment;
    }

    public customer() {
    }

    @Override
    public Long getPK() {
        return c_custkey;
    }

    @Override
    public Long getKey(String keyName) {
        if (Objects.equals(keyName, "c_custkey")) return c_custkey;
        else if (Objects.equals(keyName, "c_nationkey") || Objects.equals(keyName, "n_nationkey")) return n_nationkey;
        else throw new RuntimeException("No " + keyName + " getKey function!");
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
