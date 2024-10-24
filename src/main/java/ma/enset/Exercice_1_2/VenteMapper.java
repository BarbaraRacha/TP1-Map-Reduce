package ma.enset.Exercice_1_2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VenteMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(",");

        String[] word_prix = words[3].trim().split(" ");
        String prix = word_prix[0];

        String[] word_date = words[0].trim().split("-");
        String date = word_date[2];

        context.write(new Text(date+" "+words[1]), new IntWritable(Integer.parseInt(prix)));
    }
}
