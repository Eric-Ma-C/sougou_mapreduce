package map;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @Date: 2019/10/24 14:53
 * @Author: EricMa
 * @Description: todo:每一个map输出当前文件块内关键词的频度
 */
public class CountWordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    //使用hash表存储关键词和该词的频次
    HashMap<String, Long> map = new HashMap<>();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //分离各项数据，以‘\t’为间隔标志
        String fields[] = value.toString().split("\t");


        if (fields.length != 6) {
            return;
        }

        String keyWord = fields[2];


        long count=map.getOrDefault(keyWord,-1L);
        if (count==-1L)//判断该词是否已存在于hash表中
            map.put(keyWord,1L);//不存在，加入新词
        else
            map.replace(keyWord,count+1);//存在，词频加一

    }


    @Override
    protected void cleanup(Mapper<LongWritable, Text,  Text , LongWritable>.Context context) throws IOException, InterruptedException {
        //将当前文件块内关键词的频度输出给reducer
        for (String keyWord : map.keySet()) {
            context.write(new Text(keyWord), new LongWritable(map.get(keyWord)));
        }
    }


}