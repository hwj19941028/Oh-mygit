public class DataProcess extends UDF {
    //申明代表月份的map
    private static Map<String,String> map = new HashMap<>();
    static {
        //在静态代码块中给map put元素
        map.put("Jan","01");
        map.put("Feb","02");
        map.put("Mar","03");
        map.put("Apr","04");
        map.put("May","05");
        map.put("Jun","06");
        map.put("Jul","07");
        map.put("Aug","08");
        map.put("Sep","09");
        map.put("Oct","10");
        map.put("Nov","11");
        map.put("Dec","12");
    }


    public Text evaluate(Text text){
        //数据类型 17-Jan
        //切分数据  将数据切分为年份  和 月份
        String[] split = text.toString().split("-");
        //获取到需要的月份类型
        String month=null;
        String s=null;
		  //数据中有一些不规范的日期,年份和月份反过来了 这里我们判断一下,把他纠正
        if (split[1].length()==3){
            month = map.get(split[1]);
            if (split[0].length() < 2) {

                s = "200" + split[0] + "-" + month;
            } else {
                s = "20" + split[0] + "-" + month;
            }
        }else{
            month = map.get(split[0]);
            //根据月份位数的不同  来拼接成日期
            if (split[1].length() < 2) {

                s = "200" + split[1] + "-" + month;
            } else {
                s = "20" + split[1] + "-" + month;
            }
        }
        if (month==null|| "".equals(month)){
            return new Text("");
        }

        //将日期字符串转换为date
        DateFormat df = new SimpleDateFormat("yyyy-MM");
        Date parse = null;
        try {
            parse = df.parse(s);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //获取到当前时间 毫秒
        long l = System.currentTimeMillis();
        //计算时间差
        long l1 = l - parse.getTime();
        Long lin = 1000 * 60 * 60 * 24 * 365L;
        //使用DecimalFormat 来保留两位小数
        DecimalFormat decimalFormat = new DecimalFormat("0.00");//格式化小数
        //获取到年份返回
        String num = decimalFormat.format((float) l1 / lin);//返回的是String类型

        return   new Text(num)  ;

    }

}
