package org.tallison.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MapUtil {
    public static <K extends Comparable<? super K>,
            V extends Comparable<? super V>> Map<K, V> sortByDescendingValue(Map<K, V> map ) {
        List<Map.Entry<K, V>> list =
                new LinkedList<>( map.entrySet() );
        Collections.sort( list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
            {
                int c =  o2.getValue().compareTo(o1.getValue());
                if (c == 0) {
                    return o1.getKey().compareTo(o2.getKey());
                }
                return c;
            }
        } );

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list)
        {
            result.put( entry.getKey(), entry.getValue() );
        }
        return result;
    }
}
