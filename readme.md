### hive 新加自定义配置说明
1. 固定配置添加  
    hive.exec.pre.hooks=org.github.luikia.hive.lineage.LineageExecuteWithHookContext;  
    hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.LineageLogger;  
    hive.lineage.store.classes=org.github.luikia.hive.lineage.store.Neo4JLineageEdgeStore;  
2. 其他配置说明  
    hive.lineage.neo4j.url:(string)neo4j的服务地址,例如:http://xxx.xxx.xxx.xxx:7474  
    hive.lineage.neo4j.username:(string)neo4j用户名  
    hive.lineage.neo4j.password:(string)neo4j密码  
    hive.lineage.runsql.enable:(boolean:true)是否真正执行sql,如果为false则只进行sql血缘判断  
    hive.lineage.enable:(boolean:false)sql血缘检测是否有效  
    hive.exec.auto.engine:(boolean:false)是否支持自动选择执行引擎  
    hive.exec.spark.limit:(double:1.0)当内存使用比例小于此值时,使用spark引擎执行,否则使用mr执行,此值实际使用时应在(0,1]区间 
    hive.sql.file.name:hive文件名称