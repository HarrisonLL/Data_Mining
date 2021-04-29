package com.eecs476;
import java.io.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import java.util.*;
import java.text.*;

public class FrequentItemsets {
    public static final String CONF_Support = "conf.support";
    public static int support;
    public static int K;
    public static String inputRatings;
    public static String outputPath;
    

    public static class Mapper1
    extends Mapper<LongWritable, Text, Text, IntWritable>{
      public void map(LongWritable key, Text value, Context context
      ) throws IOException, InterruptedException {
        String val = value.toString();
        int idx = val.indexOf(",");
        String ids = val.substring(idx+1, val.length());
        String[] idList = ids.split(",");
        for (String id :idList) {
            context.write(new Text(id), new IntWritable(1));
        }
      }
    }


    public static class Reducer1
    extends Reducer<Text, IntWritable, Text, IntWritable> {
    
      public void setup(Context context) throws IOException, InterruptedException {
        support = context.getConfiguration().getInt(CONF_Support, -1);
      }
    
      public void reduce(Text key, Iterable<IntWritable> values,
          Context context) throws IOException, InterruptedException
      { 
        int count = 0;
        for (IntWritable val: values){
            count = count + val.get();
        }
    
        if (count >= support) {
            context.write(new Text(key.toString()), new IntWritable(count));
        }
      }
    }


    public static class MapperK
    extends Mapper<LongWritable, Text, Text, IntWritable>{
    
        private Set<String> Candidates = new HashSet<>();
        
        public void setup(Context context) throws IOException, InterruptedException {
            // read candidates from cache file
            URI[] candidateFiles = context.getCacheFiles();
            // make sure there was at least one file added to cache
            if (candidateFiles != null && candidateFiles.length > 0) {
                // loop over every file in the cache
                int count = 0;
                for (URI candidateFile : candidateFiles) {
                    Path inFile = new Path(candidateFile.getPath());
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(inFile)));
                    String line;
                    while ((line = br.readLine())!= null){
                        String[] lineArr = line.split(",");
                        String candidate = lineArr[0];
                        Candidates.add(candidate);
                    }
                    br.close();
                    if (count == 0) {break;}
                }
            }
        }
        
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] valueList = value.toString().split(",");
            for (int i=0; i<valueList.length; i++) {
                for (int j=i+1; j<valueList.length; j++) {
                    if (Candidates.contains(valueList[i]) && Candidates.contains(valueList[j])) {
                        String pair = valueList[i] + "," + valueList[j];
                        context.write(new Text(pair), new IntWritable(1));
                    }
                }
            }
        }
      
    }


    public static class ReducerK
      extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void setup(Context context) throws IOException, InterruptedException {
            support = context.getConfiguration().getInt(CONF_Support, -1);
          }
    
        public void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException
        { 
            int count = 0;
            for (IntWritable val:values) {
                count = count + val.get();
            }
            if (count >= support) {
                context.write(key, new IntWritable(count));
            }
                    
        }
    }


    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        for(int i = 0; i < args.length; ++i) {
            if (args[i].equals("-k")) {
                K =  Integer.parseInt(args[++i]);
            } else if (args[i].equals("-s")) {
                support =  Integer.parseInt(args[++i]);
            } else if (args[i].equals("--ratingsFile")) {
                inputRatings = args[++i];
            } else if (args[i].equals("--outputScheme")) {
                outputPath = args[++i];
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }
      
          if (outputPath == null || inputRatings == null) {
              throw new RuntimeException("Either outputpath or input path are not defined");
          }

        
        // read from original dataset to generate frequent-1 itemset
        Configuration conf1 = new Configuration();
        conf1.set("mapred.textoutputformat.separator", ",");
        conf1.set("mapreduce.job.queuename", "eecs476w21");
        conf1.setInt(CONF_Support, support);         
        
        Job Job1  = Job.getInstance(conf1, "Job1");
        Job1.setJarByClass(FrequentItemsets.class);
        Job1.setNumReduceTasks(1);
        Job1.setMapperClass(Mapper1.class);
        Job1.setReducerClass(Reducer1.class);
        Job1.setMapOutputKeyClass(Text.class);
        Job1.setMapOutputValueClass(IntWritable.class);
        Job1.setOutputKeyClass(Text.class);
        Job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(Job1, new Path(inputRatings));
        FileOutputFormat.setOutputPath(Job1, new Path(outputPath+"1"));
        Job1.waitForCompletion(true);

        // read from the original dataset and pass frequent-1 itemset to generate frequent-2 itemset

        Configuration conf2 = new Configuration();
        conf2.set("mapred.textoutputformat.separator", ",");
        conf2.set("mapreduce.job.queuename", "eecs476w21");
        conf2.setInt(CONF_Support, support);
        conf2.setInt("mapreduce.task.timeout", 6000000);       
        
        Job Job2  = Job.getInstance(conf1, "Job2");
        Job2.setJarByClass(FrequentItemsets.class);
        Job2.setNumReduceTasks(1);
        Job2.setMapperClass(MapperK.class);
        Job2.setReducerClass(ReducerK.class);
        Job2.setMapOutputKeyClass(Text.class);
        Job2.setMapOutputValueClass(IntWritable.class);
        Job2.setOutputKeyClass(Text.class);
        Job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(Job2, new Path(inputRatings));
        FileOutputFormat.setOutputPath(Job2, new Path(outputPath+"2"));
        // attach the cache files(candidates) to the context object
        URI[] cachedFilesList = {new Path(outputPath + "1/part-r-00000").toUri()};
        Job2.setCacheFiles(cachedFilesList);
        Job2.waitForCompletion(true);
            

    
    }
}
