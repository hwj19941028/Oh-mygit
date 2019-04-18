CREATE DATABASE car_used;

CREATE TABLE car_used_info (
 title string,
 city string,
 car_num string,
 offer string,
 original_price string,
 card_time string,
 mileage string,
 card_place string,
 displacement string,
 transmission_case string,
 OWNER string,
 time string,
 label string,
 car_image string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL inpath '/root/used_car/data' overwrite INTO TABLE car_used_info;

CREATE TEMPORARY FUNCTION huatec_date AS 'com.huatec.udf.DataProcess';

SELECT
 title,
 city,
 car_num,
 regexp_extract (
  offer,
  '[1-9]\\d*.\\d*|0.\\d*[1-9]\\d*',
  0
 ) AS offer,
 regexp_extract (
  original_price,
  '[1-9]\\d*.\\d*|0.\\d*[1-9]\\d*',
  0
 ) AS original_price,
 card_time,
 huatec_date (card_time) AS car_age,
 regexp_extract (
  mileage,
  '[1-9]\\d*.\\d*|0.\\d*[1-9]\\d*',
  0
 ) AS mileage,
 card_place,
 displacement,
 transmission_case,
 OWNER,
 label
FROM
 car_used_info
LIMIT 10;

#创建表   解析 一些字段
CREATE TABLE car_used_info_process (
 title string,
 city string,
 car_num string,
 offer DOUBLE,
 original_price DOUBLE,
 card_time string,
 car_age DOUBLE,
 mileage DOUBLE,
 card_place string,
 displacement string,
 transmission_case string,
 OWNER string,
 time string,
 label string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE car_used_info_process SELECT
 title,
 city,
 car_num,
 regexp_extract (
  offer,
  '[1-9]\\d*.\\d*|0.\\d*[1-9]\\d*',
  0
 ) AS offer,
 regexp_extract (
  original_price,
  '[1-9]\\d*.\\d*|0.\\d*[1-9]\\d*',
  0
 ) AS original_price,
 card_time,
 huatec_date (card_time) AS car_age,
 regexp_extract (
  mileage,
  '[1-9]\\d*.\\d*|0.\\d*[1-9]\\d*',
  0
 ) AS mileage,
 card_place,
 displacement,
 transmission_case,
 OWNER,
 time,
 label
FROM
 car_used_info;

SELECT
 city,
 "car_age",
 avg(car_age),
 max(car_age),
 min(car_age)
FROM
 car_used_info_process
GROUP BY
 city
UNION ALL
 SELECT
  'all',
  'car_age',
  avg(car_age),
  max(car_age),
  min(car_age)
 FROM
  car_used_info_process
 UNION ALL
  SELECT
   city,
   "offer",
   avg(offer),
   max(offer),
   min(offer)
  FROM
   car_used_info_process
  GROUP BY
   city
  UNION ALL
   SELECT
    'all',
    'offer',
    avg(offer),
    max(offer),
    min(offer)
   FROM
    car_used_info_process
   UNION ALL
    SELECT
     city,
     "original_price",
     avg(original_price),
     max(original_price),
     min(original_price)
    FROM
     car_used_info_process
    GROUP BY
     city
    UNION ALL
     SELECT
      'all',
      'original_price',
      avg(original_price),
      max(original_price),
      min(original_price)
     FROM
      car_used_info_process
     UNION ALL
      SELECT
       city,
       "mileage",
       avg(mileage),
       max(mileage),
       min(mileage)
      FROM
       car_used_info_process
      GROUP BY
       city
      UNION ALL
       SELECT
        'all',
        'mileage',
        avg(mileage),
        max(mileage),
        min(mileage)
       FROM
        car_used_info_process;

#建表  获取车龄 二手价 原价 车程的 最大值最小值 和平均值
CREATE TABLE car_used_count_info (
 city string,
 type string,
 car_avg string,
 car_max string,
 car_min string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE car_used_count_info SELECT
 city,
 "car_age",
 avg(car_age),
 max(car_age),
 min(car_age)
FROM
 car_used_info_process
GROUP BY
 city
UNION ALL
 SELECT
  'all',
  'car_age',
  avg(car_age),
  max(car_age),
  min(car_age)
 FROM
  car_used_info_process
 UNION ALL
  SELECT
   city,
   "offer",
   avg(offer),
   max(offer),
   min(offer)
  FROM
   car_used_info_process
  GROUP BY
   city
  UNION ALL
   SELECT
    'all',
    'offer',
    avg(offer),
    max(offer),
    min(offer)
   FROM
    car_used_info_process
   UNION ALL
    SELECT
     city,
     "original_price",
     avg(original_price),
     max(original_price),
     min(original_price)
    FROM
     car_used_info_process
    GROUP BY
     city
    UNION ALL
     SELECT
      'all',
      'original_price',
      avg(original_price),
      max(original_price),
      min(original_price)
     FROM
      car_used_info_process
     UNION ALL
      SELECT
       city,
       "mileage",
       avg(mileage),
       max(mileage),
       min(mileage)
      FROM
       car_used_info_process
      GROUP BY
       city
      UNION ALL
       SELECT
        'all',
        'mileage',
        avg(mileage),
        max(mileage),
        min(mileage)
       FROM
        car_used_info_process;

#  计算不同车龄的二手车数量,  和不同车龄的车程
CREATE TABLE count_mile_by_age (
 car_age INT,
 car_count INT,
 mileage_avg DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE count_mile_by_age SELECT
 car_age,
 count(*),
 CAST(
  avg(mileage) AS DECIMAL (18, 2)
 ) AS mileage_avg
FROM
 (
  SELECT
   round(car_age, 0) AS car_age,
   mileage
  FROM
   car_used_info_process
 )
GROUP BY
 car_age;

#计算随着车龄的增长,残值率的变化
CREATE TABLE residual_by_age (
 car_age INT,
 residual_avg DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE residual_by_age SELECT
 car_age,
 CAST(
  avg(residual) AS DECIMAL (18, 2)
 ) AS residual_avg
FROM
 (
  SELECT
   round(car_age, 0) AS car_age,
   offer / original_price AS residual
  FROM
   car_used_info_process
 )
GROUP BY
 car_age;

#计算不同车程时,车辆的残值率
CREATE TABLE residual_by_mile (
 mileage INT,
 residual_avg DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE residual_by_mile SELECT
 mileage,
 CAST(
  avg(residual) AS DECIMAL (18, 2)
 ) AS residual_avg
FROM
 (
  SELECT
   round(mileage, 0) AS mileage,
   offer / original_price AS residual
  FROM
   car_used_info_process
 )
GROUP BY
 mileage;

#二手车行驶里程分布
CREATE TABLE count_by_mileage (
 city string,
 0to1_car INT,
 1to2_car INT,
 2to3_car INT,
 3to4_car INT,
 4to5_car INT,
 5to6_car INT,
 6to7_car INT,
 7to8_car INT,
 8to9_car INT,
 9to10_car INT,
 10to11_car INT,
 11to12_car INT,
 12to13_car INT,
 13to14_car INT,
 14to15_car INT,
 15to16_car INT,
 16to17_car INT,
 17to18_car INT,
 18to19_car INT,
 19to20_car INT,
 20_car INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE count_by_mileage SELECT
 "all",
 sum(
  CASE
  WHEN mileage <= 1 THEN
   1
  ELSE
   0
  END
 ) AS 0to1_car,
 sum(
  CASE
  WHEN mileage <= 2
  AND mileage > 1 THEN
   1
  ELSE
   0
  END
 ) AS 1to2_car,
 sum(
  CASE
  WHEN mileage <= 3
  AND mileage > 2 THEN
   1
  ELSE
   0
  END
 ) AS 2to3_car,
 sum(
  CASE
  WHEN mileage <= 4
  AND mileage > 3 THEN
   1
  ELSE
   0
  END
 ) AS 3to4_car,
 sum(
  CASE
  WHEN mileage <= 5
  AND mileage > 4 THEN
   1
  ELSE
   0
  END
 ) AS 4to5_car,
 sum(
  CASE
  WHEN mileage <= 6
  AND mileage > 5 THEN
   1
  ELSE
   0
  END
 ) AS 5to6_car,
 sum(
  CASE
  WHEN mileage <= 7
  AND mileage > 6 THEN
   1
  ELSE
   0
  END
 ) AS 6to7_car,
 sum(
  CASE
  WHEN mileage <= 8
  AND mileage > 7 THEN
   1
  ELSE
   0
  END
 ) AS 7to8_car,
 sum(
  CASE
  WHEN mileage <= 9
  AND mileage > 8 THEN
   1
  ELSE
   0
  END
 ) AS 8to9_car,
 sum(
  CASE
  WHEN mileage <= 10
  AND mileage > 9 THEN
   1
  ELSE
   0
  END
 ) AS 9to10_car,
 sum(
  CASE
  WHEN mileage <= 11
  AND mileage > 10 THEN
   1
  ELSE
   0
  END
 ) AS 10to11_car,
 sum(
  CASE
  WHEN mileage <= 12
  AND mileage > 11 THEN
   1
  ELSE
   0
  END
 ) AS 11to12_car,
 sum(
  CASE
  WHEN mileage <= 13
  AND mileage > 12 THEN
   1
  ELSE
   0
  END
 ) AS 12to13_car,
 sum(
  CASE
  WHEN mileage <= 14
  AND mileage > 13 THEN
   1
  ELSE
   0
  END
 ) AS 13to14_car,
 sum(
  CASE
  WHEN mileage <= 15
  AND mileage > 14 THEN
   1
  ELSE
   0
  END
 ) AS 14to15_car,
 sum(
  CASE
  WHEN mileage <= 16
  AND mileage > 15 THEN
   1
  ELSE
   0
  END
 ) AS 15to16_car,
 sum(
  CASE
  WHEN mileage <= 17
  AND mileage > 16 THEN
   1
  ELSE
   0
  END
 ) AS 16to17_car,
 sum(
  CASE
  WHEN mileage <= 18
  AND mileage > 17 THEN
   1
  ELSE
   0
  END
 ) AS 17to18_car,
 sum(
  CASE
  WHEN mileage <= 19
  AND mileage > 18 THEN
   1
  ELSE
   0
  END
 ) AS 18to19_car,
 sum(
  CASE
  WHEN mileage <= 20
  AND mileage > 19 THEN
   1
  ELSE
   0
  END
 ) AS 19to20_car,
 sum(
  CASE
  WHEN mileage > 20 THEN
   1
  ELSE
   0
  END
 ) AS 20_car
FROM
 car_used_info_process
UNION ALL
 SELECT
  city,
  sum(
   CASE
   WHEN mileage <= 1 THEN
    1
   ELSE
    0
   END
  ) AS 0to1_car,
  sum(
   CASE
   WHEN mileage <= 2
   AND mileage > 1 THEN
    1
   ELSE
    0
   END
  ) AS 1to2_car,
  sum(
   CASE
   WHEN mileage <= 3
   AND mileage > 2 THEN
    1
   ELSE
    0
   END
  ) AS 2to3_car,
  sum(
   CASE
   WHEN mileage <= 4
   AND mileage > 3 THEN
    1
   ELSE
    0
   END
  ) AS 3to4_car,
  sum(
   CASE
   WHEN mileage <= 5
   AND mileage > 4 THEN
    1
   ELSE
    0
   END
  ) AS 4to5_car,
  sum(
   CASE
   WHEN mileage <= 6
   AND mileage > 5 THEN
    1
   ELSE
    0
   END
  ) AS 5to6_car,
  sum(
   CASE
   WHEN mileage <= 7
   AND mileage > 6 THEN
    1
   ELSE
    0
   END
  ) AS 6to7_car,
  sum(
   CASE
   WHEN mileage <= 8
   AND mileage > 7 THEN
    1
   ELSE
    0
   END
  ) AS 7to8_car,
  sum(
   CASE
   WHEN mileage <= 9
   AND mileage > 8 THEN
    1
   ELSE
    0
   END
  ) AS 8to9_car,
  sum(
   CASE
   WHEN mileage <= 10
   AND mileage > 9 THEN
    1
   ELSE
    0
   END
  ) AS 9to10_car,
  sum(
   CASE
   WHEN mileage <= 11
   AND mileage > 10 THEN
    1
   ELSE
    0
   END
  ) AS 10to11_car,
  sum(
   CASE
   WHEN mileage <= 12
   AND mileage > 11 THEN
    1
   ELSE
    0
   END
  ) AS 11to12_car,
  sum(
   CASE
   WHEN mileage <= 13
   AND mileage > 12 THEN
    1
   ELSE
    0
   END
  ) AS 12to13_car,
  sum(
   CASE
   WHEN mileage <= 14
   AND mileage > 13 THEN
    1
   ELSE
    0
   END
  ) AS 13to14_car,
  sum(
   CASE
   WHEN mileage <= 15
   AND mileage > 14 THEN
    1
   ELSE
    0
   END
  ) AS 14to15_car,
  sum(
   CASE
   WHEN mileage <= 16
   AND mileage > 15 THEN
    1
   ELSE
    0
   END
  ) AS 15to16_car,
  sum(
   CASE
   WHEN mileage <= 17
   AND mileage > 16 THEN
    1
   ELSE
    0
   END
  ) AS 16to17_car,
  sum(
   CASE
   WHEN mileage <= 18
   AND mileage > 17 THEN
    1
   ELSE
    0
   END
  ) AS 17to18_car,
  sum(
   CASE
   WHEN mileage <= 19
   AND mileage > 18 THEN
    1
   ELSE
    0
   END
  ) AS 18to19_car,
  sum(
   CASE
   WHEN mileage <= 20
   AND mileage > 19 THEN
    1
   ELSE
    0
   END
  ) AS 19to20_car,
  sum(
   CASE
   WHEN mileage > 20 THEN
    1
   ELSE
    0
   END
  ) AS 20_car
 FROM
  car_used_info_process
 GROUP BY
  city;

#查询二手车原价分布 和 售价分布 
CREATE TABLE count_by_price (
 type string,
 0to_5_count INT,
 5to_10_count INT,
 10to_15_count INT,
 15to_20_count INT,
 20to_25_count INT,
 25to_30_count INT,
 30to_35_count INT,
 35to_40_count INT,
 40to_45_count INT,
 45to_50_count INT,
 50to_55_count INT,
 55to_60_count INT,
 60to_65_count INT,
 65to_70_count INT,
 70to_75_count INT,
 75to_80_count INT,
 80to_85_count INT,
 85to_90_count INT,
 90to_95_count INT,
 95to_100_count INT,
 gt_100_count INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE count_by_price SELECT
 'offer',
 sum(
  CASE
  WHEN offer <= 5 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 10
  AND offer > 5 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 15
  AND offer > 10 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 20
  AND offer > 15 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 25
  AND offer > 20 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 30
  AND offer > 25 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 35
  AND offer > 30 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 40
  AND offer > 35 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 45
  AND offer > 40 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 50
  AND offer > 45 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 55
  AND offer > 50 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 60
  AND offer > 55 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 65
  AND offer > 60 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 70
  AND offer > 65 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 75
  AND offer > 70 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 80
  AND offer > 75 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 85
  AND offer > 80 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 90
  AND offer > 85 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 95
  AND offer > 90 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer <= 95
  AND offer > 100 THEN
   1
  ELSE
   0
  END
 ),
 sum(
  CASE
  WHEN offer > 100 THEN
   1
  ELSE
   0
  END
 )
FROM
 car_used_info_process
UNION ALL
 SELECT
  'original_price',
  sum(
   CASE
   WHEN original_price <= 5 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 10
   AND original_price > 5 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 15
   AND original_price > 10 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 20
   AND original_price > 15 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 25
   AND original_price > 20 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 30
   AND original_price > 25 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 35
   AND original_price > 30 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 40
   AND original_price > 35 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 45
   AND original_price > 40 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 50
   AND original_price > 45 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 55
   AND original_price > 50 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 60
   AND original_price > 55 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 65
   AND original_price > 60 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 70
   AND original_price > 65 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 75
   AND original_price > 70 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 80
   AND original_price > 75 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 85
   AND original_price > 80 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 90
   AND original_price > 85 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 95
   AND original_price > 90 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price <= 95
   AND original_price > 100 THEN
    1
   ELSE
    0
   END
  ),
  sum(
   CASE
   WHEN original_price > 100 THEN
    1
   ELSE
    0
   END
  )
 FROM
  car_used_info_process;