package dataStructure;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class index {
    public String indexName;
    public String PKName;
    public ArrayList<index> parent = new ArrayList<index>();
    public ArrayList<index> child = new ArrayList<index>();
    public ConcurrentHashMap<Long, baseTable> liveIndex = new ConcurrentHashMap<Long, baseTable>();
    public ConcurrentHashMap<Long, baseTable> nonLiveIndex = new ConcurrentHashMap<Long, baseTable>();


    public ConcurrentHashMap<Long, Integer> countS = new ConcurrentHashMap<Long, Integer>();

    public ArrayList<indexChild> r_rcIndexes = new ArrayList<indexChild>();


    public boolean isRoot=false;
    public boolean isLeaf=false;
    public index(String indexName, String PKName) {
        this.indexName = indexName;
        this.PKName = PKName;
    }



    public boolean isRoot() {
        return isRoot;
    }

    public void setRoot(boolean root) {
        isRoot = root;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }


}
