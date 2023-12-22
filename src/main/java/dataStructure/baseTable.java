package dataStructure;

import java.lang.reflect.Field;
import java.util.ArrayList;

public abstract class baseTable {
    public static Long specialValue=new Long(-1);
    public baseTable() {

    }
    public abstract Long getPK();

    public abstract Long getKey(String keyName);
    public abstract ArrayList<String> getAssertionKeyNames();
    public abstract Long getAssertionKeyValues(String assertionKeyName);
    public abstract void setAssertionKeys(String assertionKeyName,Long assertionKeyValue);
    public String toString(){
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
