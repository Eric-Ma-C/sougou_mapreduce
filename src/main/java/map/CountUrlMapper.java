package map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

/**
 * @Date: 2019/10/29 15:59
 * @Author: EricMa
 * @Description: todo:每一个map输出当前文件块内所有url及其频度
 */
public class CountUrlMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    //使用hash表存储url和该url的频次
    HashMap<String, Long> map = new HashMap<>();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //分离各项数据，以‘\t’为间隔标志
        String fields[] = value.toString().split("\t");

        if (fields.length != 6) {
            return;
        }

        String url = fields[5];//url

        long count=map.getOrDefault(url,-1L);
        if (count==-1L)//判断该url是否已存在于hash表中
            map.put(url,1L);//不存在，加入新url
        else
            map.replace(url,count+1);//存在，url访问次数加一
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text,  Text , LongWritable>.Context context) throws IOException, InterruptedException {
        //将当前文件块内url的访问次数输出给reducer
        for (String keyWord : map.keySet()) {
            context.write(new Text(keyWord), new LongWritable(map.get(keyWord)));
        }
    }
}
