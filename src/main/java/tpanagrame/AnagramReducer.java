package tpanagrame;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AnagramReducer  extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context
    ) throws IOException, InterruptedException {
        String anagramlist = "";
        int length = 0;
        for(Text val : values){
            anagramlist+= "," + val.toString();
            length++;
        }
        if(length>1){
            System.out.println(anagramlist);
            result.set(anagramlist);
            context.write(key,result);
        }
    }
}
