package tpanagrame;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

public class AnagramMapper extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text();
    private Text sortedText = new Text();
    private Text orginalText = new Text();

    protected  void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            char[] wordChars = word.toString().toCharArray();
            Arrays.sort(wordChars);
            String sortedWord = new String(wordChars);
            sortedText.set(sortedWord);
            orginalText.set(word);
            context.write(sortedText, orginalText);
        }
    }
}


