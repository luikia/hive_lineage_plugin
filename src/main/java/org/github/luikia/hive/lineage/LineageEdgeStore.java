package org.github.luikia.hive.lineage;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public abstract class LineageEdgeStore {
    private static final Logger LOG = LoggerFactory.getLogger(LineageEdgeStore.class);

    public LineageEdgeStore() {}

    public abstract void init(Configuration conf);

    public abstract void store(List<Edge> edges);


    public static Stream<LineageEdgeStore> getEdgeStores(final Configuration conf) {
        final String storeclass = conf.getTrimmed("hive.lineage.store.classes");
        if (StringUtils.isEmpty(storeclass)) return Collections.<LineageEdgeStore>emptyList().stream();
        String[] classes = StringUtils.split(storeclass, ",");
        return IntStream.range(0, classes.length).mapToObj(i -> {
            LineageEdgeStore store;
            try {
                store = (LineageEdgeStore) ConstructorUtils.invokeConstructor(
                        ClassUtils.getClass(StringUtils.trim(classes[i])));
                store.init(conf);
            } catch (Exception ex) {
                store = null;
                LOG.error("construct store error", ex);
            }
            return store;

        }).filter(Objects::nonNull);

    }

}
