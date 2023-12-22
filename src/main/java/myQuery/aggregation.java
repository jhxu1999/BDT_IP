package myQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import dataStructure.getResult;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class aggregation extends KeyedProcessFunction<String, getResult, ArrayList<Tuple2<String,Double>>> {
    private MapState<String, Double> addMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        addMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("mapState", Types.STRING, Types.DOUBLE)
        );
    }

    @Override
    public void processElement(getResult resultTuple, KeyedProcessFunction<String, getResult, ArrayList<Tuple2<String, Double>>>.Context ctx, Collector<ArrayList<Tuple2<String, Double>>> out) throws Exception {
        if (!addMapState.contains(resultTuple.n_name)) {
            addMapState.put(resultTuple.n_name, resultTuple.revenue);
        } else {
            Double curRevenue = addMapState.get(resultTuple.n_name);
            addMapState.put(resultTuple.n_name, curRevenue + resultTuple.revenue);
        }

        ArrayList<Tuple2<String, Double>> result = new ArrayList<>();
        for (Map.Entry<String, Double> entry : addMapState.entries()) {
            Tuple2<String, Double> tuple = new Tuple2<>(entry.getKey(), entry.getValue());
            if(!tuple.f1.equals(0.0))
            {
                result.add(tuple);
            }
        }
        Collections.sort(result, new Comparator<Tuple2<String, Double>>() {
                    @Override
                    public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
                        return o2.f1.compareTo(o1.f1); // 倒序排序
                    }
                }
        );
        out.collect(result);
    }
}
