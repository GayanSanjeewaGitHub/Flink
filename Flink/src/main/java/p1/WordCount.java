package p1;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;  // for basic data abstraction in our code
import org.apache.flink.api.java.ExecutionEnvironment; //for our execution environment 
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2; //tuple type of variable 
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCount
{
    public static void main(String[] args)
            throws Exception
    {  //setup the environment
        ExecutionEnvironment        env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment stream_env= StreamExecutionEnvironment.getExecutionEnvironment();
       //make parameters available for web interface...for later part this is helpfull for getting files through  the command lines
        ParameterTool params = ParameterTool.fromArgs(args);

        //we deal with many nodes ,so the parameters in  the param object needs to be availble for globally
        env.getConfig().setGlobalJobParameters(params);
        // read one line at a time
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<String> filtered = text.filter(new FilterFunction<String>()

        {
            public boolean filter(String value)
            {
                return value.startsWith("N");
            }
        });
        // returns a tuple of (names,1)--see the class below
        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());
        //choose 0th one due to it's group by name ,then take sum 
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[] { 0 }).sum(1);
        if (params.has("output"))
        {
            counts.writeAsCsv(params.get("output"), "\n", " ");

            env.execute("WordCount Example");
        }
    }

    public static final class Tokenizer
            implements MapFunction<String, Tuple2<String, Integer>>
    {
        public Tuple2<String, Integer> map(String value)
        {
            return new Tuple2(value, Integer.valueOf(1));
        }
    }
}