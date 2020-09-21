package org.github.luikia.hive.lineage;

import com.google.gson.stream.JsonWriter;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class Vertex {
    private static final Logger LOG = LoggerFactory.getLogger(Vertex.class);

    public enum Type {
        COLUMN, TABLE
    }

    private Vertex.Type type;
    private String label;
    private String database;
    private String table;
    private String column;
    private String comment;

    public static Vertex of(String label, Vertex.Type type, String comment) {
        return new Vertex(label, type, comment);
    }

    private Vertex(String label, Vertex.Type type, String comment) {
        this.label = label;
        this.type = type;
        String[] col_split = StringUtils.split(label, ".");
        if (type == Type.COLUMN && col_split.length == 3) {
            this.database = col_split[0];
            this.table = col_split[1];
            this.column = col_split[2];
        } else if (type == Type.TABLE && col_split.length == 2) {
            this.database = col_split[0];
            this.table = col_split[1];
            this.column = StringUtils.EMPTY;
        }
        this.comment = comment;
    }

    @Override
    public int hashCode() {
        return label.hashCode() + type.hashCode() * 3;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getComment() {
        return Objects.isNull(comment) ? StringUtils.EMPTY : comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String toJson() {
        StringBuilderWriter sw = new StringBuilderWriter();
        JsonWriter writer = new JsonWriter(sw);
        try {
            writer.beginObject();
            writer.name("name").value(this.column);
            writer.name("database").value(this.database);
            writer.name("table").value(this.table);
            writer.name("comment").value(this.comment);
            writer.name("label").value(this.label);
            writer.endObject();
            writer.close();
        } catch (Exception ex) {
            LOG.error("to json errot", ex);
        }
        return sw.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Vertex)) {
            return false;
        }
        Vertex vertex = (Vertex) obj;
        return label.equals(vertex.label) && type == vertex.type;
    }
}
