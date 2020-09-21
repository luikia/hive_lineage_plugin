package org.github.luikia.hive.lineage;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.*;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.*;

public class LineageExecuteWithHookContext implements ExecuteWithHookContext {

    private static final HashSet<String> OPERATION_NAMES = Sets.newHashSet(
            HiveOperation.QUERY.getOperationName(),
            HiveOperation.CREATETABLE_AS_SELECT.getOperationName(),
            HiveOperation.ALTERVIEW_AS.getOperationName(),
            HiveOperation.CREATEVIEW.getOperationName()
    );

    @Override
    public void run(HookContext hookContext) throws Exception {
        HiveConf conf = hookContext.getConf();
        if (!conf.getBoolean("hive.lineage.enable", false)) {
            return;
        }
        QueryPlan plan = hookContext.getQueryPlan();
        LineageCtx.Index index = hookContext.getIndex();
        SessionState ss = SessionState.get();
        if (Objects.nonNull(ss) && Objects.nonNull(index)
                && OPERATION_NAMES.contains(plan.getOperationName())
                && !plan.isExplain()) {
            try {
                List<Edge> edges = getEdges(plan, index, conf);
                if (CollectionUtils.isEmpty(edges)) {
                    return;
                }
                LineageEdgeStore.getEdgeStores(conf).forEach(s -> s.store(edges));
            } catch (Throwable t) {
                // Don't fail the query just because of any lineage issue.
                log("Failed to log lineage graph, query is not affected\n"
                        + org.apache.hadoop.util.StringUtils.stringifyException(t));
            }
        }
        if (!conf.getBoolean("hive.lineage.runsql.enable", true)) {
            this.log("hive.lineage.runsql.enable is false,job not run,lineage will reflash");
            throw new Exception("hive.lineage.runsql.enable is false,job not run");
        }
    }

    /**
     * Logger an error to console if available.
     */
    private void log(String error) {
        SessionState.LogHelper console = SessionState.getConsole();
        if (console != null) {
            console.printError(error);
        }
    }

    /**
     * Based on the final select operator, find out all the target columns.
     * For each target column, find out its sources based on the dependency index.
     */
    private List<Edge> getEdges(QueryPlan plan, LineageCtx.Index index, HiveConf conf) {
        LinkedHashMap<String, ObjectPair<SelectOperator,
                Table>> finalSelOps = index.getFinalSelectOps();
        Map<String, Vertex> vertexCache = Maps.newLinkedHashMap();
        String hqlId = conf.getTrimmed("hive.sql.id");
        List<Edge> edges = Lists.newArrayList();
        for (ObjectPair<SelectOperator,
                org.apache.hadoop.hive.ql.metadata.Table> pair : finalSelOps.values()) {
            List<FieldSchema> fieldSchemas = plan.getResultSchema().getFieldSchemas();
            SelectOperator finalSelOp = pair.getFirst();
            org.apache.hadoop.hive.ql.metadata.Table t = pair.getSecond();
            String destTableName = null;
            List<String> colNames = null;
            if (t != null) {
                destTableName = t.getDbName() + "." + t.getTableName();
                fieldSchemas = t.getCols();
            } else {
                // Based on the plan outputs, find out the target table name and column names.
                for (WriteEntity output : plan.getOutputs()) {
                    Entity.Type entityType = output.getType();
                    if (entityType == Entity.Type.TABLE
                            || entityType == Entity.Type.PARTITION) {
                        t = output.getTable();
                        destTableName = t.getDbName() + "." + t.getTableName();
                        List<FieldSchema> cols = t.getCols();
                        if (CollectionUtils.isNotEmpty(cols)) {
                            colNames = Utilities.getColumnNamesFromFieldSchema(cols);
                        }
                        break;
                    }
                }
            }
            Map<ColumnInfo, LineageInfo.Dependency> colMap = index.getDependencies(finalSelOp);
            List<LineageInfo.Dependency> dependencies = Objects.nonNull(colMap) ? Lists.newArrayList(colMap.values()) : null;
            int fields = fieldSchemas.size();
            if (Objects.isNull(t)) {
                return Collections.emptyList();
            }
            if (Objects.nonNull(colMap) && fields < colMap.size()) {
                // Dynamic partition keys should be added to field schemas.
                List<FieldSchema> partitionKeys = t.getPartitionKeys();
                int dynamicKeyCount = colMap.size() - fields;
                int keyOffset = partitionKeys.size() - dynamicKeyCount;
                if (keyOffset >= 0) {
                    fields += dynamicKeyCount;
                    for (int i = 0; i < dynamicKeyCount; i++) {
                        FieldSchema field = partitionKeys.get(keyOffset + i);
                        fieldSchemas.add(field);
                        if (Objects.nonNull(colNames)) {
                            colNames.add(field.getName());
                        }
                    }
                }
            }
            if (CollectionUtils.isEmpty(dependencies) || dependencies.size() != fields) {
                log("Result schema has " + fields
                        + " fields, but we don't get as many dependencies");
            } else {
                // Go through each target column, generate the lineage edges.
                Set<Vertex> targets = Sets.newLinkedHashSet();
                for (int i = 0; i < fields; i++) {
                    Vertex target = getOrCreateVertex(vertexCache,
                            getTargetFieldName(i, destTableName, colNames, fieldSchemas),
                            Vertex.Type.COLUMN, fieldSchemas.get(i).getComment());
                    targets.add(target);
                    LineageInfo.Dependency dep = dependencies.get(i);
                    addEdge(vertexCache, edges, dep.getBaseCols(), target,
                            dep.getExpr(), hqlId, Edge.Type.PROJECTION);
                }
                Set<LineageInfo.Predicate> conds = index.getPredicates(finalSelOp);
                if (CollectionUtils.isNotEmpty(conds)) {
                    conds.forEach(cond -> addEdge(vertexCache, edges, cond.getBaseCols(),
                            Sets.newLinkedHashSet(targets), cond.getExpr(), hqlId,
                            Edge.Type.PREDICATE));
                }
            }
        }
        return edges;
    }

    private void addEdge(Map<String, Vertex> vertexCache, List<Edge> edges,
                         Set<LineageInfo.BaseColumnInfo> srcCols, Vertex target, String expr, String hql, Edge.Type type) {
        Set<Vertex> targets = Sets.newLinkedHashSet();
        targets.add(target);
        addEdge(vertexCache, edges, srcCols, targets, expr, hql, type);
    }

    /**
     * Find an edge from all edges that has the same source vertices.
     * If found, add the more targets to this edge's target vertex list.
     * Otherwise, create a new edge and add to edge list.
     */
    private void addEdge(Map<String, Vertex> vertexCache, List<Edge> edges,
                         Set<LineageInfo.BaseColumnInfo> srcCols, Set<Vertex> targets, String expr, String hql, Edge.Type type) {
        Set<Vertex> sources = createSourceVertices(vertexCache, srcCols);
        Edge edge = findSimilarEdgeBySources(edges, sources, expr, hql, type);
        if (Objects.isNull(edge)) {
            edges.add(Edge.of(sources, targets, expr, hql, type));
        } else {
            edge.getTargets().addAll(targets);
        }
    }

    /**
     * Convert a list of columns to a set of vertices.
     * Use cached vertices if possible.
     */
    private Set<Vertex> createSourceVertices(
            Map<String, Vertex> vertexCache, Collection<LineageInfo.BaseColumnInfo> baseCols) {
        Set<Vertex> sources = Sets.newLinkedHashSet();
        if (CollectionUtils.isNotEmpty(baseCols)) {
            for (LineageInfo.BaseColumnInfo col : baseCols) {
                org.apache.hadoop.hive.metastore.api.Table table = col.getTabAlias().getTable();
                if (table.isTemporary()) {
                    // Ignore temporary tables
                    continue;
                }
                Vertex.Type type = Vertex.Type.TABLE;
                String tableName = table.getDbName() + "." + table.getTableName();
                FieldSchema fieldSchema = col.getColumn();
                String label = tableName;
                String comment = "";
                if (Objects.nonNull(fieldSchema)) {
                    type = Vertex.Type.COLUMN;
                    label = tableName + "." + fieldSchema.getName();
                    comment = fieldSchema.getComment();
                }
                sources.add(getOrCreateVertex(vertexCache, label, type, comment));
            }
        }
        return sources;
    }

    /**
     * Find a vertex from a cache, or create one if not.
     */
    private Vertex getOrCreateVertex(
            Map<String, Vertex> vertices, String label, Vertex.Type type, String comment) {
        Vertex vertex = vertices.get(label);
        if (Objects.isNull(vertex)) {
            vertex = Vertex.of(label, type, comment);
            vertices.put(label, vertex);
        }
        return vertex;
    }

    /**
     * Find an edge that has the same type, expression, and sources.
     */
    private Edge findSimilarEdgeBySources(
            List<Edge> edges, Set<Vertex> sources, String expr, String hql, Edge.Type type) {
        for (Edge edge : edges) {
            if (edge.getType() == type && StringUtils.equals(edge.getExpr(), expr)
                    && SetUtils.isEqualSet(edge.getSources(), sources) && StringUtils.equals(edge.getHql(), hql)) {
                return edge;
            }
        }
        return null;
    }

    /**
     * Generate normalized name for a given target column.
     */
    private String getTargetFieldName(int fieldIndex,
                                      String destTableName, List<String> colNames, List<FieldSchema> fieldSchemas) {
        String fieldName = fieldSchemas.get(fieldIndex).getName();
        String[] parts = StringUtils.split(fieldName, ".");
        if (Objects.nonNull(destTableName)) {
            String colName = parts[parts.length - 1];
            if (Objects.nonNull(colNames) && !colNames.contains(colName)) {
                colName = colNames.get(fieldIndex);
            }
            return destTableName + "." + colName;
        }
        if (parts.length == 2 && StringUtils.startsWith(parts[0], "_u")) {
            return parts[1];
        }
        return fieldName;
    }
}
