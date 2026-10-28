
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankMR {

    private static final double DAMPING = 0.85;
    private static final double BASE = 0.15;

    public static class RankMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] fields = line.split("\\t", -1);
            if (fields.length < 2 || fields.length > 3) {
                throw new IOException("输入格式错误: " + line);
            }

            String page = fields[0].trim();
            String linksText = fields[1].trim();

            List<String> links = new ArrayList<>();
            if (!linksText.isEmpty()) {
                for (String link : linksText.split(",")) {
                    if (!link.trim().isEmpty()) {
                        links.add(link.trim());
                    }
                }
            }

            // 首轮输入为：page<TAB>links
            // 后续轮次输入为：page<TAB>links<TAB>rank
            double rank = fields.length == 3
                    ? Double.parseDouble(fields[2].trim())
                    : 1.0;

            // 保留网页结构，确保没有入链的网页也不会消失。
            context.write(new Text(page),
                    new Text("L\t" + String.join(",", links)));

            // 向每个出链网页发送 PR 贡献。
            if (!links.isEmpty()) {
                double contribution = rank / links.size();
                for (String dest : links) {
                    context.write(new Text(dest),
                            new Text("P\t" +
                                    String.format(Locale.US, "%.16f",
                                            contribution)));
                }
            }
        }
    }

    public static class RankReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {

            String links = "";
            double sum = 0.0;

            for (Text value : values) {
                String[] parts = value.toString().split("\\t", 2);
                if (parts.length != 2) continue;

                if (parts[0].equals("L")) {
                    links = parts[1];
                } else if (parts[0].equals("P")) {
                    sum += Double.parseDouble(parts[1]);
                }
            }

            double newRank = BASE + DAMPING * sum;

            String output = links + "\t" +
                    String.format(Locale.US, "%.16f", newRank);

            context.write(key, new Text(output));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println(
                    "Usage: PageRankMR <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRank Iteration");

        job.setJarByClass(PageRankMR.class);
        job.setMapperClass(RankMapper.class);
        job.setReducerClass(RankReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 保证相同网页的所有贡献汇总到同一个 Reducer。
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
