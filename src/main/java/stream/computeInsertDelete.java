package stream;


import dataStructure.operation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import dataStructure.baseTable;
import tables.customer;
import tables.lineitem;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Field;

public class computeInsertDelete implements SourceFunction<dataStructure.operation> {
    public static String insertFilePath = "/home/xu/Documents/MSBD5014IP/data/insertData/";
    public static String deleteFilePath = "/home/xu/Documents/MSBD5014IP/data/deleteData/";
    public static String insertCustomerFile = insertFilePath + "customer.tbl";
    public static String insertLineitemFile = insertFilePath + "lineitem.tbl";
    public static String insertNationFile = insertFilePath + "nation.tbl";
    public static String insertOrdersFile = insertFilePath + "orders.tbl";
    public static String insertRegionFile = insertFilePath + "region.tbl";
    public static String insertSupplierFile = insertFilePath + "supplier.tbl";


    public static String deleteCustomerFile = deleteFilePath + "customer.tbl";
    public static String deleteLineitemFile = deleteFilePath + "lineitem.tbl";
    public static String deleteNationFile = deleteFilePath + "nation.tbl";
    public static String deleteOrdersFile = deleteFilePath + "orders.tbl";
    public static String deleteRegionFile = deleteFilePath + "region.tbl";
    public static String deleteSupplierFile = deleteFilePath + "supplier.tbl";

    String operation;
    Class<?> tClass;
    String filename;

    public computeInsertDelete(String operation, Class<?> tClass, String filename) {
        this.operation = operation;
        this.tClass = tClass;
        this.filename = filename;
    }

    @Override
    public void run(SourceContext<dataStructure.operation> ctx) throws Exception {
        if (operation.equals("delete")) {
            // To make sure deleting tuples are processed after inserting tuples
            if (tClass == customer.class) Thread.sleep(5000L);
            if (tClass == lineitem.class) Thread.sleep(10000L);
        }
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] split = line.split(">");
            String operation = split[0];
            String[] values = split[1].split("\\|");

            Object newTuple = tClass.getDeclaredConstructor().newInstance();
            Field[] fields = tClass.getDeclaredFields();
            for (int i = 0; i < values.length; i++) {
                fields[i].setAccessible(true);
                if (fields[i].getType() == String.class) {
                    fields[i].set(newTuple, values[i]);
                } else if (fields[i].getType() == Long.class) {
                    fields[i].set(newTuple, Long.parseLong(values[i]));
                } else if (fields[i].getType() == double.class) {
                    fields[i].set(newTuple, Double.parseDouble(values[i]));
                } else {
                    throw new RuntimeException("Invalid type: " + fields[i].getType());
                }
            }

            dataStructure.operation computationStream = new operation(operation, (baseTable) newTuple);
            ctx.collect(computationStream);
        }


    }

    @Override
    public void cancel() {

    }
}
