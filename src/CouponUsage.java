import java.io.IOException;
import java.nio.file.FileSystem;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class CouponUsage {

    // ======================
    // Job1: 统计平均间隔
    // ======================
    public static class CouponMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        //定义一个格式化日期，final表示这个变量不能被重新赋值
        //static：静态变量，属于类而不是实例，所有对象共享
        //SimpleDateFormat：Java中用于日期格式化的类
        private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        private final static IntWritable intervalValue = new IntWritable();
        private final static Text couponKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.startsWith("User_id")) return; // 跳过表头

            String[] fields = line.split("\\s+|,|\t");
            if (fields.length < 7) return;//因为表格应该有7列，如果比这个少证明切割错了

            String coupon_id = fields[2];
            String date_received = fields[5];
            String date_used = fields[6];
            //如果这几个值里边有空值，就不满足领取并使用优惠券的条件，直接退出这行
            if (coupon_id.equals("null") || date_received.equals("null") || date_used.equals("null"))
                return;

            try {
                //.parse表示使用定义好的格式将字符串解析为Date对象
                Date received = sdf.parse(date_received);
                Date used = sdf.parse(date_used);
                //getTime()以毫秒计数，转换成天数diff
                long diff = (used.getTime() - received.getTime()) / (1000 * 60 * 60 * 24);
                if (diff >= 0) {
                    couponKey.set(coupon_id);
                    intervalValue.set((int) diff);
                    context.write(couponKey, intervalValue);
                }
            } catch (ParseException e) {
                // 忽略异常
            }
        }
    }

    public static class CouponReducer extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;//某个优惠券使用天数间隔总和
            int count = 0;//某个优惠券被使用的总次数

            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            double avg = (count == 0) ? 0 : (double) sum / count;//某个优惠券平均使用天数间隔
            context.write(key, new Text(String.format("%.2f,%d", avg, count)));
        }
    }

    // ======================
    // Job2: 过滤 + 排序
    // ======================
    public static class FilterMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        private static long totalUsed = 0;
        private static boolean initialized = false;

        @Override
        protected void setup(Context context) throws IOException {
            if (!initialized) {
                // 读取总使用次数（通过 DistributedCache 或 job 参数）
                Configuration conf = context.getConfiguration();
                totalUsed = conf.getLong("totalUsed", 0);
                initialized = true;
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] parts = line.split("[,\t]");
            if (parts.length < 3) return;

            String coupon_id = parts[0];
            double avg = Double.parseDouble(parts[1]);
            int count = Integer.parseInt(parts[2]);

            if (count > totalUsed * 0.01) {
                context.write(new DoubleWritable(avg), new Text(coupon_id + "\t" + avg));
            }//让avg作为键以便于后边函数直接按照键的大小排序
        }
    }

    public static class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                context.write(new Text(parts[0]), new Text(parts[1]));
            }
        }
    }

    // ======================
    // 主程序
    // ======================
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: CouponUsage <input> <temp_output> <final_output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // ---------- Job1 ----------
        Job job1 = Job.getInstance(conf, "Coupon Usage Avg Calculation");
        job1.setJarByClass(CouponUsage.class);
        job1.setMapperClass(CouponMapper.class);
        job1.setReducerClass(CouponReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        boolean success = job1.waitForCompletion(true);
        if (!success) System.exit(1);

        // ---------- 计算总使用次数 ----------
        long totalUsed = 0;
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(outputPath, false);

        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            if (file.getPath().getName().startsWith("part-")) {
                FSDataInputStream in = fs.open(file.getPath());
                Scanner sc = new Scanner(in);
                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    String[] parts = line.split("[,\t]");
                    if (parts.length >= 3) {
                        totalUsed += Long.parseLong(parts[2]);
                    }
                }
                sc.close();
                in.close();
            }
        }

        // ---------- Job2 ----------
        Configuration conf2 = new Configuration();
        conf2.setLong("totalUsed", totalUsed);

        Job job2 = Job.getInstance(conf2, "Coupon Filter & Sort");
        job2.setJarByClass(CouponUsage.class);
        job2.setMapperClass(FilterMapper.class);
        job2.setReducerClass(SortReducer.class);

        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        //利用Shuffle阶段的自动排序：Hadoop会自动按DoubleWritable对key进行排序,按数值排序
        job2.setSortComparatorClass(DoubleWritable.Comparator.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        boolean success2 = job2.waitForCompletion(true);
        System.exit(success2 ? 0 : 1);
    }
}

