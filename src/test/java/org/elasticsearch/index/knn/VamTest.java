package org.elasticsearch.index.knn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VamTest {
    public static void main(String[] args) {
        List<Object> objs = Arrays.asList(1,2,3,4);
        System.out.println(objs);
       Float[]  f = objs.stream().map(x -> ((Number)x).floatValue()).toArray(Float[]::new);

       int i =1;
//       Map<String, Float> mp = Arrays.stream(f).collect(Collectors.toMap(l-> "apple" + Math.random(), l -> l));
//       System.out.println(mp);

        List<Float> lt = new ArrayList<>();
        Apple a = new Apple();
        Arrays.stream(f).forEach(l ->a.setAdder(l));
    }
}


class Apple {
    public List<Float> fl = new ArrayList<>();
    Apple() {

    }
    public void setAdder(Float f) {
        fl.add(f);
    }

}
