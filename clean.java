import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class clean {
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 2){
            System.err.println("Usage: clean <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(clean.class);
        job.setJobName("Clean users.csv");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(1);
        job.setMapperClass(cleanMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

