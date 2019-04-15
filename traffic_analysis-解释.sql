#创建HIVE数据库  叫 traffic_analysis
CREATE DATABASE traffic_analysis;
#使用刚才创建的数据库
USE traffic_analysis;
#创建数据表 traffic_data  
CREATE TABLE traffic_data (
	GSID string, #卡口ID
	CAEPLATE string, #车牌
	PLATECOORD string, #号牌种类
	PASSTIME string, #通过时间
	CARBRAND string, #车辆品牌
	CARCOLOR string, #车身颜色
	PLATECOLOR string, #号牌颜色
	SPEED INT, #车速
	DRIVEDIR INT, #行驶方向
	CARSTATE string, #车辆状态
	IMGID1 string, #图片路径
	null1 string, #空值(去掉 )
	null2 string, #空值(去掉)
	DRIVEWAY INT, #车道
	LOCATTIONID string, #地点编码
	CAPTUREDIR INT # 设备抓拍方向
)#定义数据的默认分隔符为','   每条数据将会以','拆分字段 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

# 从HDFS 将/traffic_analysis/1349020800000.txt  加载数据到 traffic_data 表
LOAD DATA inpath '/traffic_analysis/1349020800000.txt' INTO TABLE traffic_data;
#设置参数开启 动态分区（dynamic partition）
SET hive.exec.dynamic. PARTITION . MODE = nonstrict;#默认为strict
SET hive.exec.dynamic. PARTITION = TRUE; #默认为false

#创建分区表 traffic_data_partition
CREATE TABLE traffic_data_partition (
	GSID string,	#卡口ID
	CAEPLATE string, #车牌
	PLATECOORD string, #号牌种类
	passdate string, #通过日期
	time string, #通过时间
	CARBRAND string, #车辆品牌
	CARCOLOR string, #车身颜色
	PLATECOLOR string, #号牌颜色
	SPEED INT, #车速
	DRIVEDIR INT, #行驶方向
	CARSTATE string, #车辆状态
	IMGID1 string, #图片路径
	DRIVEWAY INT, #车道
	LOCATTIONID string, #地点编码
	CAPTUREDIR INT # 设备抓拍方向
) # 定义表的分区  按日期分区     指定分隔符为Hive默认的'\001'
PARTITIONED BY (datetime string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001';

#通过查询traffic_data表中的数据往traffic_data_partition表中插入数据,并实现动态分区
INSERT overwrite TABLE traffic_data_partition PARTITION (datetime) SELECT
	GSID,
	CAEPLATE,
	PLATECOORD,
	#2012-10-04 16:44:13 001 切分前日期格式
	split (PASSTIME, ' ') [ 0 ] AS passdate, #使用split函数,根据空格切分PASSTIME为数组,截取数组第一个元素 2012-10-04
	split (PASSTIME, ' ') [ 1 ] AS time, #使用split函数,根据空格切分PASSTIME为数组,截取数组第一个元素 16:44:13
	CARBRAND,
	CARCOLOR,
	PLATECOLOR,
	SPEED,
	DRIVEDIR,
	CARSTATE,
	IMGID1,
	DRIVEWAY,
	LOCATTIONID,
	CAPTUREDIR,
	#使用split函数,根据空格切分PASSTIME为数组,截取数组第一个元素 2012-10-04 作为分区字段
	split (PASSTIME, ' ') [ 0 ]
FROM
	traffic_data;

#计算天过车量,卡点数,全天机动车,外埠机动车 8点到9点的过车量
#天过车量:当天所有卡口过车的中数量
#卡点数:当天运作的卡点数量
#全天机动车:当天出行的机动车数量统计
#外埠机动车: 指外省的机动车出行总数量
#8点到9点的过车量: 8点到9点的过车数量
CREATE TABLE traffic_count AS SELECT
	COUNT(*) AS daycount,  #天过车量
	COUNT(DISTINCT gsid) AS kadian, #卡点数
	COUNT(DISTINCT CAEPLATE) AS daycar, #全天机动车
	sum(
		CASE
		WHEN substr(DISTINCT CAEPLATE, 1, 1) != '鲁' THEN
			1
		ELSE
			0
		END
	) AS waibu, #外埠机动车计算规则为: 根据车牌划分,车牌号只要不是'鲁'开头的,则划为外埠机动车
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 8
		AND split (time, ':') [ 0 ]< 9 THEN
			1
		ELSE
			0
		END
	) AS 8To9_car,#使用split拆分时间,计算通过时间为大于8点小于9点的过车数量
	passdate
FROM
	traffic_data_partition
GROUP BY
	passdate;#通过日期进行分组

#创建中间表traffic_belongplace_middle 为后续查询做准备
CREATE TABLE traffic_belongplace_middle AS SELECT
	caeplate,
	(
		CASE
		WHEN substr(caeplate, 1, 1) = '鲁' THEN #车牌号如果是以'鲁'开头,则为山东省车辆,记录车牌号第二个字符
			substr(caeplate, 2, 1)
		WHEN substr(caeplate, 1, 1) = '济' THEN#车牌号如果是以'济'开头,则为山东省军区车辆,记录为济南军区
			'济南军区'
		WHEN substr(caeplate, 1, 1) = '海' THEN#车牌号如果是以'海'开头,则为山东省青岛海军车辆,记录为济南军区
			'济南军区'
		WHEN substr(caeplate, 1, 2) = '无牌' THEN#车牌号为无牌,则为无牌车辆
			'无牌'
		ELSE
			'外地'  #其他为外地车辆
		END
	) AS belong_place, #根据车牌号来划分归属地
	passdate, 
	time,
	PLATECOORD,
	PLATECOLOR,
	LOCATTIONID
FROM
	traffic_data_partition;


#本省外市同行机动车归属地统计
#鲁A 济南，鲁B 青岛，鲁C 淄博，鲁D 枣庄，鲁E 东营，鲁F 烟台，
#鲁G 潍坊，鲁H 济宁，鲁J 泰安，鲁K 威海，鲁L 日照，鲁M 滨州，
#鲁N 德州，鲁P 聊城，鲁Q 临沂，鲁R 菏泽 鲁S 莱芜，鲁U 青岛增补 鲁Y烟台增补
#鲁W 省直机关职工私家车 归为济南
CREATE TABLE traffic_belongplace_province AS SELECT
	passdate,
	count(caeplate) AS count, #总过车量
	sum(
		CASE
		WHEN belong_place != 'A'
		AND belong_place != 'W'
		AND belong_place != '外地' THEN
			1
		ELSE
			0
		END
	) AS waishi,  #归属地不为 A W 外地 的  统称为本省外地车辆
	sum(
		CASE
		WHEN belong_place = 'A' THEN
			1
		WHEN belong_place = 'W' THEN
			1
		ELSE
			0
		END
	) AS jinan,  #A W 为济南
	sum(
		CASE
		WHEN belong_place = 'B' THEN
			1
		WHEN belong_place = 'U' THEN
			1
		ELSE
			0
		END
	) AS qingdao, #  B U 为青岛
	sum(
		CASE
		WHEN belong_place = 'K' THEN
			1
		WHEN belong_place = 'V' THEN
			1
		ELSE
			0
		END
	) AS weihai, # K V 为威海
	sum(
		CASE
		WHEN belong_place = 'F' THEN
			1
		WHEN belong_place = 'Y' THEN
			1
		ELSE
			0
		END
	) AS yantai, # F Y 为烟台 
	sum(
		CASE
		WHEN belong_place = 'C' THEN
			1
		ELSE
			0
		END
	) AS zibo, # C 为淄博
	sum(
		CASE
		WHEN belong_place = 'D' THEN
			1
		ELSE
			0
		END
	) AS zhaozhuang, # D 为枣庄
	sum(
		CASE
		WHEN belong_place = 'E' THEN
			1
		ELSE
			0
		END
	) AS dongying, # E 为东营
	sum(
		CASE
		WHEN belong_place = 'G' THEN
			1
		ELSE
			0
		END
	) AS weifang, #G 为潍坊
	sum(
		CASE
		WHEN belong_place = 'H' THEN
			1
		ELSE
			0
		END
	) AS jining, #H 为济宁
	sum(
		CASE
		WHEN belong_place = 'J' THEN
			1
		ELSE
			0
		END
	) AS taian, # J 为泰安
	sum(
		CASE
		WHEN belong_place = 'L' THEN
			1
		ELSE
			0
		END
	) AS rizhao, # L 为 日照
	sum(
		CASE
		WHEN belong_place = 'M' THEN
			1
		ELSE
			0
		END
	) AS laiwu, # M 为莱芜
	sum(
		CASE
		WHEN belong_place = 'N' THEN
			1
		ELSE
			0
		END
	) AS dezhou, #N 为德州
	sum(
		CASE
		WHEN belong_place = 'P' THEN
			1
		ELSE
			0
		END
	) AS liaocheng, # P 为聊城
	sum(
		CASE
		WHEN belong_place = 'Q' THEN
			1
		ELSE
			0
		END
	) AS linyi, # Q  临沂
	sum(
		CASE
		WHEN belong_place = 'R' THEN
			1
		ELSE
			0
		END
	) AS heze, # R 为菏泽
	sum(
		CASE
		WHEN belong_place = '无牌' THEN
			1
		ELSE
			0
		END
	) AS wupai, # 无牌车辆
	sum(
		CASE
		WHEN belong_place = '济南军区' THEN
			1
		ELSE
			0
		END
	) AS junqv # 军区车辆
FROM
	(  #查询中间表中的数据 查询 通过日期 车牌 归属地
		SELECT
			passdate,
			caeplate,
			belong_place
		FROM
			traffic_belongplace_middle
		GROUP BY
			passdate,
			caeplate,
			belong_place
	) a
GROUP BY
	passdate;
#按天外省机动车总量统计
#通过车牌号的首字符来确定车辆的所属省
CREATE TABLE traffic_belongplace_other_province AS SELECT
	passdate,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '鲁' THEN
			0
		WHEN substr(CAEPLATE, 1, 1) = '无' THEN
			0
		WHEN substr(CAEPLATE, 1, 1) = '济' THEN
			0
		WHEN substr(CAEPLATE, 1, 1) = '青' THEN
			0
		ELSE
			1
		END
	) AS other_province_count,  #  其他省出行车辆总和
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '浙' THEN
			1
		ELSE
			0
		END
	) AS zhejiang,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '闽' THEN
			1
		ELSE
			0
		END
	) AS fujian,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '粤' THEN
			1
		ELSE
			0
		END
	) AS guangdong,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '津' THEN
			1
		ELSE
			0
		END
	) AS tianjin,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '冀' THEN
			1
		ELSE
			0
		END
	) AS hebei,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '晋' THEN
			1
		ELSE
			0
		END
	) AS shanxi,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '蒙' THEN
			1
		ELSE
			0
		END
	) AS neimenggu,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '辽' THEN
			1
		ELSE
			0
		END
	) AS liaoning,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '吉' THEN
			1
		ELSE
			0
		END
	) AS jilin,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '黑' THEN
			1
		ELSE
			0
		END
	) AS heilongjiang,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '沪' THEN
			1
		ELSE
			0
		END
	) AS shanghai,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '苏' THEN
			1
		ELSE
			0
		END
	) AS jiangsu,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '皖' THEN
			1
		ELSE
			0
		END
	) AS anhui,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '赣' THEN
			1
		ELSE
			0
		END
	) AS jiangxi,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '豫' THEN
			1
		ELSE
			0
		END
	) AS henan,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '鄂' THEN
			1
		ELSE
			0
		END
	) AS hubei,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '湘' THEN
			1
		ELSE
			0
		END
	) AS hunan,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '桂' THEN
			1
		ELSE
			0
		END
	) AS guangxizhuangzu,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '琼' THEN
			1
		ELSE
			0
		END
	) AS hainan,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '渝' THEN
			1
		ELSE
			0
		END
	) AS chongqing,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '川' THEN
			1
		ELSE
			0
		END
	) AS sichuan,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '贵' THEN
			1
		ELSE
			0
		END
	) AS guizhou,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '云' THEN
			1
		ELSE
			0
		END
	) AS yunnan,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '藏' THEN
			1
		ELSE
			0
		END
	) AS xizang,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '陕' THEN
			1
		ELSE
			0
		END
	) AS shan_xi,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '甘' THEN
			1
		ELSE
			0
		END
	) AS gansu,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '青' THEN
			1
		ELSE
			0
		END
	) AS qinghai,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '宁' THEN
			1
		ELSE
			0
		END
	) AS ningxia,
	sum(
		CASE
		WHEN substr(CAEPLATE, 1, 1) = '新' THEN
			1
		ELSE
			0
		END
	) AS xinjiang
FROM
	traffic_data_partition
GROUP BY
	passdate;

#按天计算每小时出行的机动车的数量
#通过切分通过的时间,来获取该车俩通过卡点的时间,从而计算出每小时出行的机动车的数量
CREATE TABLE traffic_time_hours AS SELECT
	passdate,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 0
		AND split (time, ':') [ 0 ]< 1 THEN
			1
		ELSE
			0
		END
	) AS 0To1_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 1
		AND split (time, ':') [ 0 ]< 2 THEN
			1
		ELSE
			0
		END
	) AS 1To2_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 2
		AND split (time, ':') [ 0 ]< 3 THEN
			1
		ELSE
			0
		END
	) AS 2To3_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 3
		AND split (time, ':') [ 0 ]< 4 THEN
			1
		ELSE
			0
		END
	) AS 3To4_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 4
		AND split (time, ':') [ 0 ]< 5 THEN
			1
		ELSE
			0
		END
	) AS 4To5_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 5
		AND split (time, ':') [ 0 ]< 6 THEN
			1
		ELSE
			0
		END
	) AS 5To6_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 6
		AND split (time, ':') [ 0 ]< 7 THEN
			1
		ELSE
			0
		END
	) AS 6To7_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 7
		AND split (time, ':') [ 0 ]< 8 THEN
			1
		ELSE
			0
		END
	) AS 7To8_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 8
		AND split (time, ':') [ 0 ]< 9 THEN
			1
		ELSE
			0
		END
	) AS 8To9_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 9
		AND split (time, ':') [ 0 ]< 10 THEN
			1
		ELSE
			0
		END
	) AS 9To10_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 10
		AND split (time, ':') [ 0 ]< 11 THEN
			1
		ELSE
			0
		END
	) AS 10To11_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 11
		AND split (time, ':') [ 0 ]< 12 THEN
			1
		ELSE
			0
		END
	) AS 11To12_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 12
		AND split (time, ':') [ 0 ]< 13 THEN
			1
		ELSE
			0
		END
	) AS 12To13_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 13
		AND split (time, ':') [ 0 ]< 14 THEN
			1
		ELSE
			0
		END
	) AS 13To14_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 14
		AND split (time, ':') [ 0 ]< 15 THEN
			1
		ELSE
			0
		END
	) AS 14To15_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 15
		AND split (time, ':') [ 0 ]< 16 THEN
			1
		ELSE
			0
		END
	) AS 15To16_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 16
		AND split (time, ':') [ 0 ]< 17 THEN
			1
		ELSE
			0
		END
	) AS 16To17_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 17
		AND split (time, ':') [ 0 ]< 18 THEN
			1
		ELSE
			0
		END
	) AS 17To18_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 18
		AND split (time, ':') [ 0 ]< 19 THEN
			1
		ELSE
			0
		END
	) AS 18To19_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 19
		AND split (time, ':') [ 0 ]< 20 THEN
			1
		ELSE
			0
		END
	) AS 19To20_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 20
		AND split (time, ':') [ 0 ]< 21 THEN
			1
		ELSE
			0
		END
	) AS 20To21_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 21
		AND split (time, ':') [ 0 ]< 22 THEN
			1
		ELSE
			0
		END
	) AS 21To22_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 22
		AND split (time, ':') [ 0 ]< 23 THEN
			1
		ELSE
			0
		END
	) AS 22To23_car,
	sum(
		CASE
		WHEN split (time, ':') [ 0 ]>= 23 THEN
			1
		ELSE
			0
		END
	) AS 23To24_car
FROM
	traffic_data_partition
GROUP BY
	passdate;	

# 通行机动车类型分布
#通过号牌种类来确定车辆的类型
#代  码	名   称	车牌颜色	     代  码	名   称	车牌颜色
#01	大型汽车号牌	黄底黑字	13	农用运输车号牌	黄底黑字黑框线
#03	使馆汽车号牌	黑底白字、红“ 使”字	15	挂车号牌	黄底黑字黑框线
#04	领馆汽车号牌	黑底白字、红“ 领”字	16	教练汽车号牌	黄底黑字黑框线
#05	境外汽车号牌	黑底白/红字	17	教练摩托车号牌	黄底黑字黑框线
#06	外籍汽车号牌	黑底白字	18	试验汽车号牌	　
#07	两、三轮摩托车号牌	黄底黑字	19	试验摩托车号牌	　
#08	轻便摩托车号牌	蓝底白字	20	临时入境汽车号牌	白底红字黑“临 时入境”
#09	使馆摩托车号牌	黑底白字、红“ 使”字	21	临时入境摩托车号牌	白底红字黑” 临时入境”
#10	领馆摩托车号牌	黑底白字、红“ 领”字	22	临时行驶车号牌	白底黑字黑框线
#11	境外摩托车号牌	黑底白字	23	警用汽车号牌	　
#12	外籍摩托车号牌	黑底白字	24	警用摩托号牌	　
#99	其它号牌	　	　	　	　
CREATE TABLE Vehicle_type_count AS SELECT
	passdate,
	count(caeplate),
	sum(
		CASE
		WHEN platecoord = 01 THEN
			1
		ELSE
			0
		END
	) AS daxing,
	sum(
		CASE
		WHEN platecoord = 02 THEN
			1
		ELSE
			0
		END
	) AS xiaoxing,
	sum(
		CASE
		WHEN platecoord = 03 THEN
			1
		ELSE
			0
		END
	) AS shiguan,
	sum(
		CASE
		WHEN platecoord = 04 THEN
			1
		ELSE
			0
		END
	) AS lingguan,
	sum(
		CASE
		WHEN platecoord = 05 THEN
			1
		ELSE
			0
		END
	) AS jingwai,
	sum(
		CASE
		WHEN platecoord = 06 THEN
			1
		ELSE
			0
		END
	) AS waiji,
	sum(
		CASE
		WHEN platecoord = 07 THEN
			1
		ELSE
			0
		END
	) AS sanlunmotuo,
	sum(
		CASE
		WHEN platecoord = 08 THEN
			1
		ELSE
			0
		END
	) AS qingbianmotuo,
	sum(
		CASE
		WHEN platecoord = 09 THEN
			1
		ELSE
			0
		END
	) AS shituanmotuo,
	sum(
		CASE
		WHEN platecoord = 10 THEN
			1
		ELSE
			0
		END
	) AS lingguanmotuo,
	sum(
		CASE
		WHEN platecoord = 11 THEN
			1
		ELSE
			0
		END
	) AS jingwaimotuo,
	sum(
		CASE
		WHEN platecoord = 12 THEN
			1
		ELSE
			0
		END
	) AS waijimotuo,
	sum(
		CASE
		WHEN platecoord = 13 THEN
			1
		ELSE
			0
		END
	) AS nongyongyunshu,
	sum(
		CASE
		WHEN platecoord = 14 THEN
			1
		ELSE
			0
		END
	) AS tuolaji,
	sum(
		CASE
		WHEN platecoord = 15 THEN
			1
		ELSE
			0
		END
	) AS guache,
	sum(
		CASE
		WHEN platecoord = 16 THEN
			1
		ELSE
			0
		END
	) AS jiaoliaoqiche,
	sum(
		CASE
		WHEN platecoord = 17 THEN
			1
		ELSE
			0
		END
	) AS jiaolianmotuo,
	sum(
		CASE
		WHEN platecoord = 18 THEN
			1
		ELSE
			0
		END
	) AS shiyanqiche,
	sum(
		CASE
		WHEN platecoord = 19 THEN
			1
		ELSE
			0
		END
	) AS shiyanmotuo,
	sum(
		CASE
		WHEN platecoord = 20 THEN
			1
		ELSE
			0
		END
	) AS linshirujingqiche,
	sum(
		CASE
		WHEN platecoord = 21 THEN
			1
		ELSE
			0
		END
	) AS linShiRuJingMoTuo,
	sum(
		CASE
		WHEN platecoord = 22 THEN
			1
		ELSE
			0
		END
	) AS linshixingshiche,
	sum(
		CASE
		WHEN platecoord = 23 THEN
			1
		ELSE
			0
		END
	) AS jingyongqiche,
	sum(
		CASE
		WHEN platecoord = 24 THEN
			1
		ELSE
			0
		END
	) AS jingyongmotuo,
	sum(
		CASE
		WHEN platecoord = 99 THEN
			1
		ELSE
			0
		END
	) AS qita,
	sum(
		CASE
		WHEN platecoord = 'CPT_NULL' THEN
			1
		ELSE
			0
		END
	) AS wuleixing
FROM
	(  #从traffic_data_partition表中查询 通过日期,车牌号,号牌种类字段 并分组
		SELECT
			passdate,
			caeplate,
			PLATECOORD
		FROM
			traffic_data_partition
		GROUP BY
			passdate,
			caeplate,
			PLATECOORD
	) a
GROUP BY
	passdate;

#近30天本省外市高频出行机动车分布
#从traffic_belongplace_province 本省外市机动车统计表中查询数据
#使用date_sub 函数  将日期前推30天
CREATE TABLE Vehicle_other_city_Month AS SELECT
	passdate,
	sum(qingdao) AS c_qingdao,
	sum(weihai) AS c_weihai,
	sum(yantai) AS c_yantai,
	sum(zibo) AS c_zibo,
	sum(zhaozhuang) AS c_zhaozhuang,
	sum(dongying) AS c_dongying,
	sum(weifang) AS c_weifang,
	sum(jining) AS c_jining,
	sum(taian) AS c_taian,
	sum(rizhao) AS c_rizhao,
	sum(laiwu) AS c_laiwu,
	sum(dezhou) AS c_dezhou,
	sum(liaocheng) AS c_liaocheng,
	sum(linyi) AS c_linyi,
	sum(heze) AS c_heze,
	sum(wupai) AS c_wupai,
	sum(junqv) AS c_junqv
FROM
	traffic_belongplace_province
WHERE
	passdate BETWEEN date_sub(passdate, 30)
AND passdate
GROUP BY
	passdate;



