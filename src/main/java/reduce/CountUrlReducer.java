package reduce;

import javafx.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;



/**
 * @Date: 2019/10/29 16:09
 * @Author: EricMa
 * @Description: todo:将所有url访问次数合并到一起,筛选出访问次数最大的K条url
 */
public class CountUrlReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private static int K = 10;//选出访问次数最大的K条url
    private static long urlCountTotal = 0;//累计总的url访问次数

    //小顶堆，容量K，用于快速删除url访问次数最小的元素（url）
    PriorityQueue<Pair<String, Long>> minHeap = new PriorityQueue<>((p1, p2) -> (int) (p1.getValue() - p2.getValue()));

    //每次传入的参数为key相同的values的集合
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long total = 0;

        for (LongWritable count : values) {
            //依次取出每个mapper统计的url的访问次数，加起来
            total += count.get();
        }

        urlCountTotal += total;
        Pair<String, Long> tmp = new Pair<>(key.toString(), total);
        minHeap.add(tmp);//向小顶堆插入新的url访问次数
        if (minHeap.size() > K)//若小顶堆容量达到要求的上限
            minHeap.poll();//删除堆顶最小的元素，保持TopK
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<Pair<String, Long>> list = new ArrayList<>();
        //从小顶堆中取出数据，便于排序
        for (Pair<String, Long> p : minHeap)
            list.add(p);

        //对url访问次数前K个元素排序
        Collections.sort(list, ((p1, p2) -> (int) (p2.getValue() - p1.getValue())));

        //reducer的输出，按url访问次数排好序的TopK的url
        for (Pair<String, Long> t : list)
            context.write(new Text(t.getKey()), new LongWritable(t.getValue()));

        System.out.println("url访问总数="+urlCountTotal);
    }
}