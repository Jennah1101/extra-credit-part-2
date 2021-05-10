import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class cleanMapper
        extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static final int len = 13;
    
    //( id BIGINT, login STRING, created_at String, type STRING, fake STRING, deleted STRING, country_code STRING)
    private static final int[] neededIdx = {0, 1, 3, 4, 5, 6, 9};

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if(key.get() != 0){
            String[] entries = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            String newEntry = "";
            
            if(entries.length == len && entries[0].compareTo("\\N") != 0){
                for(int i = 0; i < neededIdx.length - 1; i ++){
                    newEntry += entries[neededIdx[i]] + ",";
                }
                
                newEntry += entries[neededIdx[neededIdx.length-1]];
                context.write(new Text(newEntry), NullWritable.get());
            }
        }
    }
}

