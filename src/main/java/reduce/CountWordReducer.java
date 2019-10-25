package reduce;

import java.io.IOException;
import java.util.*;


import javafx.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Date: 2019/10/24 14:54
 * @Author: EricMa
 * @Description: todo:将所有关键字搜索频次合并到一起,筛选出频次最大的K条关键词
 */
public class CountWordReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    public static int K = 300;//选出频次最大的K条关键词


    //小顶堆，容量K，用于快速删除词频最小的元素
    PriorityQueue<Pair<String, Long>> minHeap = new PriorityQueue<>((p1, p2) -> (int) (p1.getValue() - p2.getValue()));

    //每次传入的参数为key相同的values的集合
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long total = 0;
        int c = 0;//测试，可删
        for (LongWritable count : values) {
            //依次取出每个mapper统计的关键词key的频次，加起来
            total += count.get();
            c++;
        }
        if (c > 2)
            c = 5;

        Pair<String, Long> tmp = new Pair<>(key.toString(), total);
        minHeap.add(tmp);//向小顶堆插入新的关键词词频
        if (minHeap.size() > K)//若小顶堆容量达到要求的上限
            minHeap.poll();//删除堆顶最小的元素，保持TopK

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<Pair<String, Long>> list = new ArrayList<>();
        //从小顶堆中取出数据，便于排序
        for (Pair<String, Long> p : minHeap)
            list.add(p);

        //对搜索词频前K个元素排序
        Collections.sort(list, ((p1, p2) -> (int) (p2.getValue() - p1.getValue())));

        //reducer的输出，按搜索词频排好序的TopK关键词
        for (Pair<String, Long> t : list)
            context.write(new Text(t.getKey()), new LongWritable(t.getValue()));
    }
}