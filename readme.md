### hive 新加自定义配置说明
1. 固定配置添加  
    hive.exec.pre.hooks=org.github.luikia.hive.lineage.LineageExecuteWithHookContext;  
    hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.LineageLogger;  
    hive.lineage.store.classes=org.github.luikia.hive.lineage.store.Neo4JLineageEdgeStore;  
2. 其他配置说明  
    hive.lineage.neo4j.url:(string)neo4j的http服务地址,例如:http://xxx.xxx.xxx.xxx:7474  
    hive.lineage.neo4j.username:(string)neo4j用户名  
    hive.lineage.neo4j.password:(string)neo4j密码  
    hive.lineage.runsql.enable:(boolean:true)是否真正执行SQL,如果为false则只进行SQL血缘判断  
    hive.lineage.enable:(boolean:false)SQL血缘检测是否有效  
    hive.sql.id:hive脚本id号,唯一的SQL脚本标识