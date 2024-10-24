package ma.enset.Exercice_2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class LogsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();

        int somme = 0;
        while (iterator.hasNext()){
            somme += iterator.next().get();
        }

        context.write(key, new IntWritable(somme));
    }
}
