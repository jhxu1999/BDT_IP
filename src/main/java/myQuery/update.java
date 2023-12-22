package myQuery;

import dataStructure.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


import dataStructure.operation;
import tables.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class update extends KeyedProcessFunction<String, operation, getResult> {

    private ValueState<getIndex> relationState;

    @Override
    public void open(Configuration parameters) throws Exception {
        relationState = getRuntimeContext().getState(
                new ValueStateDescriptor<getIndex>("relationState", getIndex.class));
    }

    @Override
    public void processElement(operation operation, KeyedProcessFunction<String, operation, getResult>.Context ctx, Collector<getResult> out) throws Exception {
        getIndex getIndex = relationState.value();
        if (getIndex == null) {
            getIndex = new getIndex();
        }
        if (operation.operation.equals("insert")) {
            if (operation.data.getClass() == customer.class) {
                insert(operation.data, getIndex.customerIndex, getIndex, out);
            } else if (operation.data.getClass() == lineitem.class) {
                insert(operation.data, getIndex.lineitemIndex, getIndex, out);
            } else if (operation.data.getClass() == nation.class) {
                insert(operation.data, getIndex.nationIndex, getIndex, out);
            } else if (operation.data.getClass() == orders.class) {
                insert(operation.data, getIndex.ordersIndex, getIndex, out);
            } else if (operation.data.getClass() == supplier.class) {
                insert(operation.data, getIndex.supplierIndex, getIndex, out);
            } else if (operation.data.getClass() == region.class) {
                insert(operation.data, getIndex.regionIndex, getIndex, out);
            } else {
                throw new RuntimeException("Wrong tuple class!");
            }

        } else if (operation.operation.equals("delete")) {

            if (operation.data.getClass() == customer.class) {
                delete(operation.data, getIndex.customerIndex, getIndex, out);
            } else if (operation.data.getClass() == lineitem.class) {
                delete(operation.data, getIndex.lineitemIndex, getIndex, out);
            } else if (operation.data.getClass() == nation.class) {
                delete(operation.data, getIndex.nationIndex, getIndex, out);
            } else if (operation.data.getClass() == orders.class) {
                delete(operation.data, getIndex.ordersIndex, getIndex, out);
            } else if (operation.data.getClass() == supplier.class) {
                delete(operation.data, getIndex.supplierIndex, getIndex, out);
            } else if (operation.data.getClass() == region.class) {
                delete(operation.data, getIndex.regionIndex, getIndex, out);
            } else {
                throw new RuntimeException("Wrong tuple class!");
            }


        } else {
            throw new RuntimeException("Operation not legal!");
        }

    }


    private void insert(baseTable data, index relation, getIndex getIndex, Collector<getResult> out) throws IOException {

        Long rPK = data.getPK();
        int childNum = relation.child.size();

        boolean allAssertionKeyNotSpecialValue = true;
        if (!relation.isLeaf) {
            relation.countS.put(rPK, 0);

            for (int i = 0; i < childNum; i++) {
                index childIndex = relation.child.get(i);
                indexChild r_rcIndex = relation.r_rcIndexes.get(i);
                ConcurrentHashMap<Long, baseTable> childLiveIndex = childIndex.liveIndex;
                String rcPKName = childIndex.PKName;
                Long rcPK = data.getKey(rcPKName);
                if (r_rcIndex.index.containsKey(rcPK)) {
                    r_rcIndex.index.get(rcPK).add(rPK);
                } else {
                    CopyOnWriteArrayList<Long> newValue = new CopyOnWriteArrayList<Long>();
                    newValue.add(rPK);
                    r_rcIndex.index.put(rcPK, newValue);
                }
                if (childLiveIndex.containsKey(rcPK)) {
                    Integer sPK = relation.countS.get(rPK) + 1;
                    relation.countS.put(rPK, sPK);
                }
            }
            if (relation.countS.get(rPK) == childNum) {
                for (int i = 0; i < childNum; i++) {
                    index childIndex = relation.child.get(i);
                    String rcPKName = childIndex.PKName;
                    Long rcPK = data.getKey(rcPKName);
                    baseTable childTuple = childIndex.liveIndex.get(rcPK);
                    ArrayList<String> assertionKeyNames = data.getAssertionKeyNames();
                    if (assertionKeyNames != null) {
                        for (String assertionKeyName : assertionKeyNames) {
                            Long assertionKeyValue = childTuple.getKey(assertionKeyName);
                            if (data.getAssertionKeyValues(assertionKeyName) == null) {
                                data.setAssertionKeys(assertionKeyName, assertionKeyValue);
                            } else if (data.getAssertionKeyValues(assertionKeyName) != assertionKeyValue
                                    || data.getAssertionKeyValues(assertionKeyName) == baseTable.specialValue) {
                                data.setAssertionKeys(assertionKeyName, baseTable.specialValue);
                                allAssertionKeyNotSpecialValue = false;
                            }

                        }
                    }
                }
            }


        }
        if (relation.isLeaf || relation.countS.get(rPK) == childNum && allAssertionKeyNotSpecialValue) {
            insertUpdate(data, relation, new getResult(data), getIndex, out);
        } else {
            relation.nonLiveIndex.put(rPK, data);

        }
        relationState.update(getIndex);

    }

    private void insertUpdate(baseTable data, index relation, getResult joinResult, getIndex getIndex, Collector<getResult> out) throws IOException {

        Long rPK = data.getPK();
        relation.liveIndex.put(rPK, data);
        if (relation.isRoot) {
            out.collect(getAllResult(joinResult, "insert", getIndex));
        } else {
            for (index parentIndex : relation.parent) {
                for (indexChild rRcIndex : parentIndex.r_rcIndexes) {
                    if (rRcIndex.childName.equals(relation.indexName)) {
                        if (rRcIndex.index.containsKey(rPK) && rRcIndex.index.get(rPK) != null && !rRcIndex.index.get(rPK).isEmpty()) {

                            for (Long parentTuplePK : rRcIndex.index.get(rPK)) {
                                Integer sPK = parentIndex.countS.get(parentTuplePK) + 1;
                                parentIndex.countS.put(parentTuplePK, sPK);
                                int childNum = parentIndex.child.size();
                                if (sPK == childNum) {
                                    boolean allAssertionKeyNotSpecialValue = true;
                                    baseTable parentTuple = parentIndex.nonLiveIndex.get(parentTuplePK);
                                    if (parentTuple==null)
                                        System.out.println();
                                    for (int i = 0; i < childNum; i++) {
                                        index parentChildIndex = parentIndex.child.get(i);
                                        String rcPKName = parentChildIndex.PKName;
                                        Long rcPK = parentTuple.getKey(rcPKName);
                                        baseTable parentChildTuple = parentChildIndex.liveIndex.get(rcPK);
                                        ArrayList<String> assertionKeyNames = parentTuple.getAssertionKeyNames();

                                        if (assertionKeyNames != null) {
                                            for (String assertionKeyName : assertionKeyNames) {
                                                Long assertionKeyValue = parentChildTuple.getKey(assertionKeyName);
                                                if (parentTuple.getAssertionKeyValues(assertionKeyName) == null) {
                                                    parentTuple.setAssertionKeys(assertionKeyName, assertionKeyValue);
                                                } else if (parentTuple.getAssertionKeyValues(assertionKeyName) != assertionKeyValue
                                                        || parentTuple.getAssertionKeyValues(assertionKeyName) == baseTable.specialValue) {
                                                    parentTuple.setAssertionKeys(assertionKeyName, baseTable.specialValue);
                                                    allAssertionKeyNotSpecialValue = false;
                                                }

                                            }
                                        }

                                    }
                                    if (allAssertionKeyNotSpecialValue) {
                                        parentIndex.nonLiveIndex.remove(parentTuplePK);

                                        insertUpdate(parentTuple, parentIndex, joinResult.join(parentTuple), getIndex, out);


                                    }
                                }
                            }
                        }

                    }
                }
            }
        }
        relationState.update(getIndex);
    }

    private void delete(baseTable data, index relation, getIndex getIndex, Collector<getResult> out) throws IOException {
        Long rPK = data.getPK();
        if (relation.liveIndex.containsKey(rPK)) {
            deleteUpdate(data, relation, new getResult(data), getIndex, out);
        } else {
            relation.nonLiveIndex.remove(rPK);
        }
        if (!relation.isRoot) {
            for (index parentIndex : relation.parent) {
                for (indexChild rRcIndex : parentIndex.r_rcIndexes) {
                    if (rRcIndex.childName.equals(relation.indexName)) {
                        rRcIndex.index.remove(rPK);
                    }

                }

            }
        }
        relationState.update(getIndex);
    }

    private void deleteUpdate(baseTable data, index relation, getResult joinResult, getIndex getIndex, Collector<getResult> out) throws IOException {

        Long rPK = data.getPK();
        relation.liveIndex.remove(rPK);
        if (relation.isRoot) {
            out.collect(getAllResult(joinResult, "delete", getIndex));
        } else {
            for (index parentIndex : relation.parent) {
                for (indexChild rRcIndex : parentIndex.r_rcIndexes) {
                    if (rRcIndex.childName.equals(relation.indexName)) {
                        if (rRcIndex.index.containsKey(rPK)) {
                            for (Long parentTuplePK : rRcIndex.index.get(rPK)) {
                                if (parentIndex.nonLiveIndex.containsKey(parentTuplePK)) {
                                    Integer sPK = parentIndex.countS.get(parentTuplePK) - 1;
                                    parentIndex.countS.put(parentTuplePK, sPK);
                                } else {
                                    baseTable parentTuple = parentIndex.liveIndex.get(parentTuplePK);
                                    Integer sPK = parentIndex.child.size() - 1;
                                    parentIndex.countS.put(parentTuplePK, sPK);
                                    parentIndex.nonLiveIndex.put(parentTuplePK, parentTuple);
                                    deleteUpdate(parentTuple, parentIndex, joinResult.join(parentTuple), getIndex, out);
                                }
                            }
                        }
                    }
                }
            }
        }
        relationState.update(getIndex);
    }

    public static getResult getAllResult(getResult joinResult, String operation, getIndex getIndex) {
        if (!joinResult.hasSupplier) {
            supplier supplierTuple = (supplier) getIndex.supplierIndex.liveIndex.get(joinResult.s_suppkey);
            joinResult.join(supplierTuple);
        }
        if (!joinResult.hasOrders) {
            orders ordersTuple = (orders) getIndex.ordersIndex.liveIndex.get(joinResult.o_orderkey);
            joinResult.join(ordersTuple);
        }
        if (!joinResult.hasCustomer) {
            customer customerTuple = (customer) getIndex.customerIndex.liveIndex.get(joinResult.c_custkey);
            joinResult.join(customerTuple);
        }
        if (!joinResult.hasNation) {
            nation nationTuple = (nation) getIndex.nationIndex.liveIndex.get(joinResult.n_nationkey);
            if (nationTuple == null)
                System.out.print(joinResult.toString() + '\n');
            joinResult.join(nationTuple);
        }
        if (!joinResult.hasRegion) {
            region regionTuple = (region) getIndex.regionIndex.liveIndex.get(joinResult.r_regionkey);
            joinResult.join(regionTuple);
        }
        if (operation.equals("delete")) {
            joinResult.revenue = -joinResult.revenue;
        }
        ;
        return joinResult;
    }

}
