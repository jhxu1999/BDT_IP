package dataStructure;

import tables.*;
import myQuery.*;

import java.lang.reflect.Field;

public class getResult {
    public double revenue;
    public double l_extendedprice;
    public double l_discount;
    public Long c_custkey;
    public String n_name;
    public Long o_orderkey;
    public Long s_suppkey;
    public Long n_nationkey;

    public Long r_regionkey;

    boolean hasLineitem = false;
    public boolean hasOrders = false;
    public boolean hasCustomer = false;
    public boolean hasNation = false;
    public boolean hasRegion = false;
    public boolean hasSupplier = false;

    public getResult() {
    }

    public getResult(baseTable newTuple) {
        this.join(newTuple);
    }

    public getResult join(baseTable newTuple) {
        if (newTuple.getClass() == customer.class) {
            hasCustomer = true;
            n_nationkey = ((customer) newTuple).n_nationkey;
        } else if (newTuple.getClass() == lineitem.class) {
            hasLineitem = true;
            l_discount = ((lineitem) newTuple).l_discount;
            l_extendedprice = ((lineitem) newTuple).l_extendedprice;
            o_orderkey = ((lineitem) newTuple).o_orderkey;
            s_suppkey = ((lineitem) newTuple).s_suppkey;
            revenue = l_extendedprice * (1 - l_discount);
        } else if (newTuple.getClass() == nation.class) {
            hasNation = true;
            n_name = ((nation) newTuple).n_name;
            r_regionkey = ((nation) newTuple).r_regionkey;
        } else if (newTuple.getClass() == orders.class) {
            hasOrders = true;
            c_custkey = ((orders) newTuple).c_custkey;
        } else if (newTuple.getClass() == supplier.class) {
            hasSupplier = true;
            n_nationkey = ((supplier) newTuple).n_nationkey;
        } else if (newTuple.getClass() == region.class) {
            hasRegion = true;
        }
        return this;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        Field[] fields = getClass().getDeclaredFields();

        for (Field field : fields) {
            field.setAccessible(true);

            try {
                String name = field.getName();
                Object value = field.get(this);

                sb.append(name).append(": ").append(value).append(", ");
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 2);
        }

        return sb.toString();
    }
}
