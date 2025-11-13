// 离线优惠券消费行为统计 MapReduce 程序
// 统计每个商家的三类行为：领取未使用、未领取普通消费、领取并使用
// Hadoop MapReduce job for counting coupon usage per merchant

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OfflineCoupon {

//Mapper 接收到的 key 是文件行的字节偏移，不用管。
//Mapper 处理的主要是 value(每行的文本内容)
    public static class CouponMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().isEmpty()) return;

            // 用逗号分隔
            String[] fields = line.split(",");
            if (fields.length < 7) return;

            String merchantId = fields[1].trim();
            String couponId = fields[2].trim();
            String dateReceived = fields[5].trim();
            String date = fields[6].trim();

            // Negative sample: received but not used
            if (!couponId.equals("null") && !dateReceived.equals("null") && date.equals("null")) {
                context.write(new Text(merchantId), new Text("NEG"));
            }
            // Normal consumption: no coupon used
            else if (couponId.equals("null") && dateReceived.equals("null") && !date.equals("null")) {
                context.write(new Text(merchantId), new Text("NORM"));
            }
            // Positive sample: coupon used
            else if (!couponId.equals("null") && !dateReceived.equals("null") && !date.equals("null")) {
                context.write(new Text(merchantId), new Text("POS"));
            }
        }
    }

    // Reducer: aggregates counts per merchant
    public static class CouponReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int neg = 0;
            int norm = 0;
            int pos = 0;

            for (Text v : values) {
                String tag = v.toString();
                switch (tag) {
                    case "NEG": neg++; break;
                    case "NORM": norm++; break;
                    case "POS": pos++; break;
                }
            }

            context.write(key, new Text(neg + "\t" + norm + "\t" + pos));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: OfflineCouponAnalysis <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Offline Coupon Analysis");

        job.setJarByClass(OfflineCoupon.class);
        job.setMapperClass(CouponMapper.class);
        job.setReducerClass(CouponReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
