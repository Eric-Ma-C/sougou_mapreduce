import map.CountUrlMapper;
import map.CountWordMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import reduce.CountUrlReducer;
import reduce.CountWordReducer;

/**
 * @Date: 2019/10/24 14:52
 * @Author: EricMa
 * @Description: todo:
 */


public class Main {
    public static void main(String[] args) throws Exception {

        countWords();    //统计词频前30的搜索关键词

        countUrls();//被访问次数前10的网址及其次数占比

    }

    private static void countWords() throws Exception {

        //        String input_dir="./data/sogou.500w.utf8";
        String input_dir = "./data/sogou.full.utf8";//input
        String outputDir = "./result/words";//output

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(new Path(outputDir));
        fs.close();

        Job job = new Job(conf, "CountWords");
        job.setMapperClass(CountWordMapper.class);
        job.setReducerClass(CountWordReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(input_dir));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outputDir));

        job.waitForCompletion(true);

    }

    private static void countUrls() throws Exception {

//        String input_dir = "./data/sogou.500w.utf8";
        String input_dir="./data/sogou.full.utf8";//input
        String outputDir = "./result/urls";//output

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(new Path(outputDir));
        fs.close();

        Job job = new Job(conf, "CountUrls");
        job.setMapperClass(CountUrlMapper.class);
        job.setReducerClass(CountUrlReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(input_dir));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outputDir));

        job.waitForCompletion(true);

    }

}