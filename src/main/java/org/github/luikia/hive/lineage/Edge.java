package org.github.luikia.hive.lineage;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.Set;

public class Edge {
    public enum Type {PROJECTION, PREDICATE}

    private Set<Vertex> sources;
    private Set<Vertex> targets;
    private String expr;
    private String hql;
    private Edge.Type type;

    public static Edge of(Set<Vertex> sources, Set<Vertex> targets, String expr, String hql, Edge.Type type) {
        return new Edge(sources, targets, expr, hql, type);
    }

    private Edge(Set<Vertex> sources, Set<Vertex> targets, String expr, String hql, Edge.Type type) {
        this.sources = sources;
        this.targets = targets;
        this.expr = expr;
        this.hql = hql;
        this.type = type;
    }

    public Set<Vertex> getSources() {
        return sources;
    }

    public void setSources(Set<Vertex> sources) {
        this.sources = sources;
    }

    public Set<Vertex> getTargets() {
        return targets;
    }

    public void setTargets(Set<Vertex> targets) {
        this.targets = targets;
    }

    public String getExpr() {
        String s = StringUtils.remove(expr, "'");
        return Objects.isNull(s) ? StringUtils.EMPTY : s;
    }

    public void setExpr(String expr) {
        this.expr = expr;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getHql() {
        return Objects.isNull(hql) ? StringUtils.EMPTY : hql;
    }

    public void setHql(String hql) {
        this.hql = hql;
    }
}
