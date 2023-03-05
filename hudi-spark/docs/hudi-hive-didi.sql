
-- 1. 创建数据库database
CREATE DATABASE IF NOT EXISTS db_hudi ;

-- 2. 使用数据库
USE db_hudi ;

-- 3. 创建表
CREATE EXTERNAL TABLE IF NOT EXISTS tbl_hudi_didi(
                                                     order_id bigint          ,
                                                     product_id int           ,
                                                     city_id int              ,
                                                     district int             ,
                                                     county int               ,
                                                     type int                 ,
                                                     combo_type int           ,
                                                     traffic_type int         ,
                                                     passenger_count int      ,
                                                     driver_product_id int    ,
                                                     start_dest_distance int  ,
                                                     arrive_time string       ,
                                                     departure_time string    ,
                                                     pre_total_fee double     ,
                                                     normal_time string       ,
                                                     bubble_trace_id string   ,
                                                     product_1level int       ,
                                                     dest_lng double          ,
                                                     dest_lat double          ,
                                                     starting_lng double      ,
                                                     starting_lat double      ,
                                                     partitionpath string     ,
                                                     ts bigint
)
    PARTITIONED BY (date_str string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hudi.hadoop.HoodieParquetInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION '/hudi-warehouse/tbl_didi_haikou' ;


-- 查看分区表分区
SHOW PARTITIONS db_hudi.tbl_hudi_didi ;

-- 5. 手动添加分区信息
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-5-22') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-5-22' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-5-23') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-5-23' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-5-24') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-5-24' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-5-25') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-5-25' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-5-26') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-5-26' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-5-27') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-5-27' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-5-28') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-5-28' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-5-29') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-5-29' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-5-30') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-5-30' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-5-31') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-5-31' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-6-1') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-6-1' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-6-2') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-6-2' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-6-3') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-6-3' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-6-4') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-6-4' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-6-5') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-6-5' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-6-6') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-6-6' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-6-7') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-6-7' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-6-8') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-6-8' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-6-9') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-6-9' ;
ALTER TABLE db_hudi.tbl_hudi_didi ADD IF NOT EXISTS PARTITION (date_str = '2017-6-10') LOCATION  '/hudi-warehouse/tbl_didi_haikou/2017-6-10' ;

-- 测试，查询数据
SET hive.mapred.mode = nonstrict ;
SELECT order_id, product_id, type, pre_total_fee, traffic_type, start_dest_distance FROM db_hudi.tbl_hudi_didi LIMIT 20;




-- 开发测试，设置运行模式为本地模式
set hive.exec.mode.local.auto=true;

set hive.exec.mode.local.auto.tasks.max=10;
set hive.exec.mode.local.auto.inputbytes.max=50000000;


-- 指标一：订单类型统计
WITH tmp AS (
    SELECT product_id, COUNT(1) AS total FROM db_hudi.tbl_hudi_didi GROUP BY product_id
)
SELECT
    CASE product_id
        WHEN 1 THEN "滴滴专车"
        WHEN 2 THEN "滴滴企业专车"
        WHEN 3 THEN "滴滴快车"
        WHEN 4 THEN "滴滴企业快车"
        END AS order_type,
    total
FROM tmp ;


-- 指标二：订单时效性统计
WITH tmp AS (
    SELECT type, COUNT(1) AS total FROM db_hudi.tbl_hudi_didi GROUP BY type
)
SELECT
    CASE type
        WHEN 0 THEN "实时"
        WHEN 1 THEN "预约"
        END AS order_type,
    total
FROM tmp ;


-- 指标三：订单交通类型统计
SELECT traffic_type, COUNT(1) AS total FROM db_hudi.tbl_hudi_didi GROUP BY traffic_type ;


-- 指标五：订单价格统计，先将价格划分区间，再统计，此处使用 WHEN函数和SUM函数
SELECT
    SUM(
            CASE WHEN pre_total_fee BETWEEN 0 AND 15 THEN 1 ELSE 0 END
        ) AS 0_15,
    SUM(
            CASE WHEN pre_total_fee BETWEEN 16 AND 30 THEN 1 ELSE 0 END
        ) AS 16_30,
    SUM(
            CASE WHEN pre_total_fee BETWEEN 31 AND 50 THEN 1 ELSE 0 END
        ) AS 31_50,
    SUM(
            CASE WHEN pre_total_fee BETWEEN 50 AND 100 THEN 1 ELSE 0 END
        ) AS 51_100,
    SUM(
            CASE WHEN pre_total_fee > 100 THEN 1 ELSE 0 END
        ) AS 100_
FROM
    db_hudi.tbl_hudi_didi ;







