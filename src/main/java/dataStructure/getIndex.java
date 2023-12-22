package dataStructure;

public class getIndex {
    public index lineitemIndex = new index("lineitem", "o_orderkey&l_linenumber");
    public index ordersIndex = new index("orders", "o_orderkey");
    public index customerIndex = new index("customer", "c_custkey");

    public index supplierIndex = new index("supplier", "s_suppkey");
    public index nationIndex = new index("nation", "n_nationkey");
    public index regionIndex = new index("region", "r_regionkey");

    public getIndex() {
        lineitemIndex.setRoot(true);
        lineitemIndex.setLeaf(false);
        ordersIndex.setRoot(false);
        ordersIndex.setLeaf(false);
        customerIndex.setRoot(false);
        customerIndex.setLeaf(false);
        supplierIndex.setRoot(false);
        supplierIndex.setLeaf(false);
        nationIndex.setRoot(false);
        nationIndex.setLeaf(false);
        regionIndex.setRoot(false);
        regionIndex.setLeaf(true);

        lineitemIndex.child.add(ordersIndex);
        lineitemIndex.child.add(supplierIndex);
        ordersIndex.parent.add(lineitemIndex);
        ordersIndex.child.add(customerIndex);
        customerIndex.parent.add(ordersIndex);
        supplierIndex.parent.add(lineitemIndex);
        supplierIndex.child.add(nationIndex);
        nationIndex.parent.add(supplierIndex);
        nationIndex.child.add(regionIndex);
        regionIndex.parent.add(nationIndex);

        indexChild lineitem_orders_Index = new indexChild(lineitemIndex.indexName, ordersIndex.indexName);
        indexChild orders_customer_Index = new indexChild(ordersIndex.indexName, customerIndex.indexName);
        indexChild lineitem_supplier_Index = new indexChild(lineitemIndex.indexName, supplierIndex.indexName);
        indexChild supplier_nation_Index = new indexChild(supplierIndex.indexName, nationIndex.indexName);
        indexChild nation_region__Index = new indexChild(nationIndex.indexName, regionIndex.indexName);

        lineitemIndex.r_rcIndexes.add(lineitem_orders_Index);
        lineitemIndex.r_rcIndexes.add(lineitem_supplier_Index);
        ordersIndex.r_rcIndexes.add(orders_customer_Index);
        supplierIndex.r_rcIndexes.add(supplier_nation_Index);
        nationIndex.r_rcIndexes.add(nation_region__Index);

    }


}
