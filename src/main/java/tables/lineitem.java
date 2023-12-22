package tables;

import dataStructure.baseTable;

import java.util.ArrayList;
import java.util.Hashtable;

public class lineitem extends baseTable {
    public Long o_orderkey;
    public Long l_partkey;
    public Long s_suppkey;
    // Primary Key
    public Long l_linenumber;
    public double l_quantity;
    public double l_extendedprice;
    public double l_discount;
    public double l_tax;
    public String l_returnflag;
    public String l_linestatus;
    public String l_shipdate;
    public String l_commitdate;
    public String l_receiptdate;
    public String l_shipinstruct;
    public String l_shipmode;
    public String l_comment;


    public lineitem(Long o_orderkey, Long l_partkey, Long s_suppkey, Long l_linenumber, double l_quantity, double l_extendedprice, double l_discount, double l_tax, String l_returnflag, String l_linestatus, String l_shipdate, String l_commitdate, String l_receiptdate, String l_shipinstruct, String l_shipmode, String l_comment) {
//        new Lineitem();
        this.o_orderkey = o_orderkey;
        this.l_partkey = l_partkey;
        this.s_suppkey = s_suppkey;
        this.l_linenumber = l_linenumber;
        this.l_quantity = l_quantity;
        this.l_extendedprice = l_extendedprice;
        this.l_discount = l_discount;
        this.l_tax = l_tax;
        this.l_returnflag = l_returnflag;
        this.l_linestatus = l_linestatus;
        this.l_shipdate = l_shipdate;
        this.l_commitdate = l_commitdate;
        this.l_receiptdate = l_receiptdate;
        this.l_shipinstruct = l_shipinstruct;
        this.l_shipmode = l_shipmode;
        this.l_comment = l_comment;
    }

    public lineitem() {
        assertionKeyNames.add("n_nationkey");
    }

    @Override
    public Long getPK() {
        return o_orderkey * 10 + l_linenumber;
    }

    @Override
    public Long getKey(String keyName) {
        if(keyName=="l_orderkey" || keyName=="o_orderkey") return o_orderkey;
        if(keyName=="s_suppkey") return s_suppkey;
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
