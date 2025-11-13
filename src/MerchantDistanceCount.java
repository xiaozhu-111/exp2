import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MerchantDistanceCount {

    // Mapper: 输入每行 → 输出 (Merchant_id, Distance)
    public static class DistanceMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // 跳过标题行
            if (line.startsWith("User_id"))
                return;

            String[] fields = line.split(",");

            if (fields.length < 7)
                return;

            String merchant = fields[1].trim();   // Merchant_id
            String distance = fields[4].trim();   // Distance

            if (merchant == null || merchant.isEmpty())
                return;

            if (distance == null || distance.isEmpty())
                distance = "NULL";

            context.write(new Text(merchant), new Text(distance));
        }
    }

    // Reducer: 统计每个商家的不同距离人数
    public static class DistanceReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // 0~10 共 11 个距离 + 1 个 NULL
            int[] count = new int[12];

            for (Text v : values) {
                String d = v.toString();

                if (d.equals("NULL")) {
                    count[11]++;     // NULL 的计数放在 index 11
                } else {
                    try {
                        int dist = Integer.parseInt(d);
                        if (dist >= 0 && dist <= 10)
                            count[dist]++;
                        else
                            count[11]++;  // 超出范围的当 NULL
                    } catch (Exception e) {
                        count[11]++;
                    }
                }
            }

            // 输出 12 个计数，用 tab 分隔
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 12; i++) {
                sb.append(count[i]);
                if (i != 11) sb.append("\t");
            }

            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Merchant Distance Count");

        job.setJarByClass(MerchantDistanceCount.class);
        job.setMapperClass(DistanceMapper.class);
        job.setReducerClass(DistanceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

