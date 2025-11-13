import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DiscountRate {

    // Mapper 阶段
    public static class DiscountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable result = new IntWritable();
        private final static Text discountKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.startsWith("User_id")) return; // 跳过表头
            String[] fields = line.split("\\s+|,|\t");
            if (fields.length < 7) return;

            String discount = fields[3];
            String date_received = fields[5];
            String date_used = fields[6];

            if (discount.equals("null") || date_received.equals("null"))
                return;

            discountKey.set(discount);

            if (!date_used.equals("null")) {
                // 优惠券被使用
                result.set(1);
            } else {
                // 优惠券未使用
                result.set(0);
            }
            context.write(discountKey, result);
        }
    }


    // Reducer 阶段
    public static class DiscountReducer extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int used = 0;
            int total = 0;
            for (IntWritable v : values) {
                total++;
                if (v.get() == 1) used++;
            }
            if (total > 0) {
                double rate = (double) used / total;
                context.write(key, new Text(String.format("%.4f", rate)));
            }
        }
    }

    // Driver 主函数
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DiscountRate <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Discount Rate Usage Analysis");

        job.setJarByClass(DiscountRate.class);
        job.setMapperClass(DiscountMapper.class);
        job.setReducerClass(DiscountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

