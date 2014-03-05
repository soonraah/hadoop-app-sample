package driver;

import mapreduce.SkypeWordCountMapper;
import mapreduce.SkypeWordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath;


/**
 * エントリポイントを持ち、ジョブの実行を担います。
 */
public class SkypeWordCountDriver extends Configured implements Tool {

    public static void main(String[] args) throws IOException {
        int exitCode = 1;
        try {
            // ToolRunner によりジョブを実行
            exitCode = ToolRunner.run(new SkypeWordCountDriver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf);

        job.setJobName("Skype ログのユーザ別の頻出単語取得");
        job.setJarByClass(SkypeWordCountDriver.class);

        // 入出力フォーマットの指定
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // MapReduce クラスの指定
        job.setMapperClass(SkypeWordCountMapper.class);
        job.setReducerClass(SkypeWordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 入出力フォーマットの指定
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 入出力ファイルパスの設定
        addInputPath(job, new Path(conf.get("input.s3.objectKey")));
        setOutputPath(job, new Path(conf.get("output.s3.objectPrefix")));

        // ジョブの実行
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }
}
