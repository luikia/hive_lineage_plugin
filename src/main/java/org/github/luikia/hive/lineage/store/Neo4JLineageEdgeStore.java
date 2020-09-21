package org.github.luikia.hive.lineage.store;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.github.luikia.hive.lineage.Edge;
import org.github.luikia.hive.lineage.LineageEdgeStore;
import org.github.luikia.hive.lineage.Vertex;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Neo4JLineageEdgeStore extends LineageEdgeStore {
    private static final Logger LOG = LoggerFactory.getLogger(Neo4JLineageEdgeStore.class);

    private static final Gson g = new Gson();

    private static Neo4J client;

    private static final String DB_TABLE_CQL_FORMAT =
            "MERGE (d:DATABASE{name:$db}) ON CREATE SET d+={name:$db,ctime:timestamp(),mtime:timestamp()} ON MATCH SET d+={mtime:timestamp()} " +
                    "MERGE (t:TABLE{name:$table,db:$db}) ON CREATE SET t+={name:$table,db:$db,ctime:timestamp(),mtime:timestamp()} ON MATCH SET t+={mtime:timestamp()} " +
                    "WITH d,t MERGE (d)-[r:HAVE_TABLE]->(t) ON CREATE SET r += {ctime:timestamp(),mtime:timestamp()} ON MATCH SET r += {mtime:timestamp()}";

    private static final String COL_CQL_FORMAT =
            "MERGE (c:COLUMN{label:$label}) ON CREATE SET c+={label:$label,name:$col,table:$table,db:$db,comment:$comment,ctime:timestamp(),mtime:timestamp()} ON MATCH SET c+={mtime:timestamp()} " +
                    "MERGE (t:TABLE{name:$table,db:$db}) with c,t MERGE (t)-[r:HAVE_COLUMN]->(c) ON CREATE SET r += {ctime:timestamp(),mtime:timestamp()} ON MATCH SET r += {mtime:timestamp()}";

    private static final String COL_DEP_COL_CQL_FORMAT =
            "MATCH (cs:COLUMN{label:$slabel}) MATCH (ct:COLUMN{label:$tlabel})" +
                    "with cs,ct MERGE (cs)-[r:DEP{expr:$expr,hql:$hql}]->(ct) ON CREATE SET r += {expr:$expr,hql:$hql," +
                    "ctime:timestamp(),mtime:timestamp()} ON MATCH SET r += {mtime:timestamp()}";

    private static final String COL_DEP_TABLE_CQL_FORMAT =
            "MATCH (cs:COLUMN{label:$slabel}) MATCH (ct:TABLE{db:$tdb,name:$ttable})" +
                    "with cs,ct MERGE (cs)-[r:DEP{expr:$expr,hql:$hql}]->(ct) ON CREATE SET r += {expr:$expr,hql:$hql," +
                    "ctime:timestamp(),mtime:timestamp()} ON MATCH SET r += {mtime:timestamp()}";


    @Override
    public void init(Configuration conf) {
        if (Objects.isNull(client)) initCLient(conf);
    }

    @Override
    public void store(List<Edge> edges) {
        edges.stream()
                .filter(e -> e.getType() == Edge.Type.PROJECTION)
                .flatMap(this::convertEdgeToCQL)
                .forEach(client::run);
    }

    private synchronized static void initCLient(Configuration conf) {
        if (Objects.nonNull(client)) return;
        String url = conf.getTrimmed("hive.lineage.neo4j.url");
        String username = conf.getTrimmed("hive.lineage.neo4j.username");
        String password = conf.getTrimmed("hive.lineage.neo4j.password");
        client = new Neo4J(url, username, password);
    }

    private Stream<Neo4jEntity> convertEdgeToCQL(Edge edge) {
        List<Neo4jEntity> cqls = Lists.newArrayListWithCapacity(edge.getSources().size() * edge.getTargets().size() + edge.getSources().size() + edge.getTargets().size());
        cqls.addAll(edge.getSources().stream().flatMap(this::convertVertex).collect(Collectors.toList()));
        cqls.addAll(edge.getTargets().stream().flatMap(this::convertVertex).collect(Collectors.toList()));
        for (Vertex s : edge.getSources()) {
            Set<Vertex> targetStream = edge.getTargets();
            cqls.addAll(targetStream.stream().filter(t -> t.getType() == Vertex.Type.COLUMN)
                    .map(t -> {
                        Neo4jEntity entity = new Neo4jEntity();
                        entity.setCql(COL_DEP_COL_CQL_FORMAT);
                        entity.setArgs(ImmutableMap.of(
                                "slabel", s.getLabel(),
                                "tlabel", t.getLabel(),
                                "expr", edge.getExpr(),
                                "hql", edge.getHql()
                        ));
                        return entity;
                    }).collect(Collectors.toList()));
            cqls.addAll(targetStream.stream().filter(t -> t.getType() == Vertex.Type.TABLE)
                    .map(t -> {
                        Neo4jEntity entity = new Neo4jEntity();
                        entity.setCql(COL_DEP_TABLE_CQL_FORMAT);
                        entity.setArgs(ImmutableMap.of(
                                "slabel", s.getLabel(),
                                "tdb", t.getDatabase(),
                                "ttable", t.getTable(),
                                "expr", StringUtils.trimToEmpty(edge.getExpr()),
                                "hql", StringUtils.trimToEmpty(edge.getHql())
                        ));
                        return entity;


                    })
                    .collect(Collectors.toList()));
        }
        return cqls.stream();
    }


    private Stream<Neo4jEntity> convertVertex(Vertex v) {
        List<Neo4jEntity> list = Lists.newArrayListWithCapacity(2);
        Neo4jEntity entity = new Neo4jEntity();
        entity.setCql(DB_TABLE_CQL_FORMAT);
        entity.setArgs(ImmutableMap.of("db", v.getDatabase(), "table", v.getTable()));
        list.add(entity);
        if (v.getType() == Vertex.Type.COLUMN) {
            Neo4jEntity cc = new Neo4jEntity();
            cc.setCql(COL_CQL_FORMAT);
            cc.setArgs(ImmutableMap.of(
                    "label", v.getLabel(),
                    "col", v.getColumn(),
                    "table", v.getTable(),
                    "db", v.getDatabase(),
                    "comment", v.getComment()

            ));
            list.add(cc);
        }
        return list.stream();
    }

    private static class Neo4J {

        private HttpClient client;
        private String url;
        private Header[] headers;

        Neo4J(String url, String username, String password) {
            this.url = url + "/db/data/cypher";
            this.client = new DefaultHttpClient();
            headers = new Header[2];
            String token = "Basic " + Base64.getEncoder().encodeToString(StringUtils.join(username, ":", password).getBytes());
            headers[0] = new BasicHeader(HttpHeaders.AUTHORIZATION, token);
            headers[1] = new BasicHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        }

        private void run(Neo4jEntity entity) {
            HttpPost post = new HttpPost(url);
            try {
                post.setEntity(new StringEntity(entity.toHttpBody()
                        , ContentType.APPLICATION_JSON)
                );
                post.setHeaders(this.headers);
                HttpResponse resp = this.client.execute(post);
                if (resp.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                    LOG.error(EntityUtils.toString(resp.getEntity()));
                }
            } catch (Exception e) {
                LOG.error("insert neo4j error,cql:" + entity.cql + ",params:" + entity.getArgs(), e);
            } finally {
                post.releaseConnection();
            }
        }


    }


    private class Neo4jEntity {
        private String cql;
        private Map<String, String> args;

        public void setCql(String cql) {
            this.cql = cql;
        }

        public Map<String, String> getArgs() {
            return args;
        }

        public void setArgs(Map<String, String> args) {
            this.args = args;
        }

        private String toHttpBody() {
            JsonObject json = new JsonObject();
            json.addProperty("query", this.cql);
            JsonElement params = g.toJsonTree(this.args);
            json.add("params", params);
            return json.toString();
        }

    }


}
