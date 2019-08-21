package hadoop.mapreduce;

import com.study.bigdata.hadoop.mapreduce.utils.ContentUtils;
import com.study.bigdata.hadoop.mapreduce.utils.LogParser;
import com.study.bigdata.hadoop.mapreduce.utils.IPParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class IPTest {

    @Test
    public void test(){
        IPParser ipParser = IPParser.getInstance();
        IPParser.RegionInfo regionInfo = ipParser.analyseIp("111.0.168.70");
        System.out.println(regionInfo.getCountry());
        System.out.println(regionInfo.getProvince());
        System.out.println(regionInfo.getCity());
    }

    LogParser logParser;

    @Before
    public void before(){
        logParser = new LogParser();
    }

    @After
    public void after(){
        logParser = null;
    }

    @Test
    public void testLogParser(){
        Map<String, String> map = logParser.parser("20946835322\u0001http://www.yihaodian.com/1/?tracker_u=2225501&type=3\u0001http://www.baidu.com/s?wd=1%E5%8F%B7%E5%BA%97&rsv_bp=0&ch=&tn=baidu&bar=&rsv_spt=3&ie=utf-8&rsv_sug3=5&rsv_sug=0&rsv_sug1=4&rsv_sug4=313&inputT=4235\u00011号店\u00011\u0001SKAPHD3JZYH9EE9ACB1NGA9VDQHNJMX1NY9T\u0001\u0001\u0001\u0001\u0001PPG4SWG71358HGRJGQHQQBXY9GF96CVU\u00012225501\u0001\\N\u0001124.79.172.232\u0001\u0001msessionid:YR9H5YU7RZ8Y94EBJNZ2P5W8DT37Q9JH,unionKey:2225501\u0001\u00012013-07-21 09:30:01\u0001\\N\u0001http://www.baidu.com/s?wd=1%E5%8F%B7%E5%BA%97&rsv_bp=0&ch=&tn=baidu&bar=&rsv_spt=3&ie=utf-8&rsv_sug3=5&rsv_sug=0&rsv_sug1=4&rsv_sug4=313&inputT=4235\u00011\u0001\u0001\\N\u0001null\u0001-10\u0001\u0001\u0001\u0001\u0001Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; MATP; Media Center PC 6.0; .NET4.0C; InfoPath.2; .NET4.0E)\u0001Win32\u0001\u0001\u0001\u0001\u0001\u0001上海市\u00011\u0001\u00012013-07-21 09:30:01\u0001上海市\u0001\u000166\u0001\u0001\u0001\u0001\u0001\\N\u0001\\N\u0001\\N\u0001\\N\u00012013-07-21");
        for (Map.Entry<String, String> entry : map.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }

    @Test
    public void testContent(){
        System.out.println(ContentUtils.getPageId("http://www.yihaodian.com/cms/view.do?topicId=19004"));
    }

    @Test
    public void testLogParserV2(){
        String str = "2013-07-21 12:56:18\t中国\t广东省\tnull\thttp://www.yihaodian.com/item/2017644_1?ref=1_1_51_search.keyword_1?utm_source=iQiyi&utm_medium=UCMJF018&utm_term=&utm_content=Video_15s_pre-roll&utm_campaign=UL_MSP-Jul_Clear_Awareness_China_Display&smt_b=C0B0A0908070605A9FBF00C";
        String[] arrs = str.split("\t");
        System.out.println(arrs[5]);

    }
}
