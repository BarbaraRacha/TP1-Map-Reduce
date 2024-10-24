package ma.enset.Exercice_2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");

        context.write(new Text(words[0]), new IntWritable(1));
        if(words[8].equals("200")){
            context.write(new Text(words[0]+"_200"), new IntWritable(1));
        }
    }
}
