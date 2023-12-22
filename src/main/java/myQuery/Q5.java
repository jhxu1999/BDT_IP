package myQuery;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import dataStructure.operation;
import stream.computeInsertDelete;
import tables.*;



public class Q5 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        String Date1 = "1996-01-01";
        String Date2 = "1997-01-01";
        String region = "EUROPE";


        //insert
        DataStream<operation> insertLineitem = env.addSource(new computeInsertDelete("insert", lineitem.class, computeInsertDelete.insertLineitemFile));
        DataStream<operation> insertOrders = env.addSource(new computeInsertDelete("insert", orders.class, computeInsertDelete.insertOrdersFile)).filter(tuple -> ((orders) tuple.data).o_orderdate.compareTo(Date1) >= 0 && ((orders) tuple.data).o_orderdate.compareTo(Date2) < 0);
        DataStream<operation> insertCustomer = env.addSource(new computeInsertDelete("insert", customer.class, computeInsertDelete.insertCustomerFile));
        DataStream<operation> insertSupplier = env.addSource(new computeInsertDelete("insert", supplier.class, computeInsertDelete.insertSupplierFile));
        DataStream<operation> insertNation = env.addSource(new computeInsertDelete("insert", nation.class, computeInsertDelete.insertNationFile));
        DataStream<operation> insertRegion = env.addSource(new computeInsertDelete("insert", tables.region.class, computeInsertDelete.insertRegionFile)).filter(tuple -> ((tables.region) tuple.data).r_name.equals(region));
        //delete
        DataStream<operation> deleteLineitem = env.addSource(new computeInsertDelete("delete", lineitem.class, computeInsertDelete.deleteLineitemFile));
        DataStream<operation> deleteCustomer = env.addSource(new computeInsertDelete("delete", customer.class, computeInsertDelete.deleteCustomerFile));
        DataStream<operation> deleteNation = env.addSource(new computeInsertDelete("delete", nation.class, computeInsertDelete.deleteNationFile));
        DataStream<operation> deleteSupplier = env.addSource(new computeInsertDelete("delete", supplier.class, computeInsertDelete.deleteSupplierFile));
        DataStream<operation> deleteRegion = env.addSource(new computeInsertDelete("delete", tables.region.class, computeInsertDelete.deleteRegionFile));
        DataStream<operation> deleteOrders = env.addSource(new computeInsertDelete("delete", orders.class, computeInsertDelete.deleteOrdersFile));


        //DataStream<DataOperation> allStream = insertLineitem.union(insertOrders).union(insertCustomer).union(insertSupplier).union(insertNation).union(insertRegion).union(deleteLineitem).union(deleteCustomer).union(deleteNation).union(deleteSupplier);
        DataStream<operation> allStream = insertLineitem.union(insertOrders).union(insertCustomer).union(insertSupplier).union(insertNation).union(insertRegion).union(deleteLineitem).union(deleteCustomer);



        allStream.keyBy(value -> "key").process(new update()).keyBy(value -> "key").process(new aggregation()).writeAsText("/home/xu/Documents/MSBD5014/result/result").setParallelism(1);
        env.execute();
    }
}