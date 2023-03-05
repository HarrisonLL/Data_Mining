package com.eecs476;
import java.io.*;
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
import java.io.IOException;
import java.util.*;
import java.text.*;

public class AssociationRules {
    private static String support;
    private static String K;
    private static String inputRatings;
    private static String outputPath;
    private static ArrayList<String> Candidates;
    private static HashMap<String, Integer> Xcount; 

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
      public void reduce(Text key, Iterable<IntWritable> values,
          Context context) throws IOException, InterruptedException
      { 
        int count = 0;
        for (IntWritable val: values){
            count = count + val.get();
        }
        int s = Integer.parseInt(support);
        if (count >= s) {
            context.write(new Text(key.toString()), new IntWritable(count));
        }
      }
    }


    public static class MapperKCand
    extends Mapper<LongWritable, Text, Text, Text>{
      public void map(LongWritable key, Text value, Context context
      ) throws IOException, InterruptedException {
        String keyValues = value.toString();
        String[] KVList = keyValues.split(",");
        if (KVList.length==2){
            context.write(new Text("#"), new Text(KVList[0]));
        } else {
            
            String newVal = KVList[KVList.length-2]; // 100,200,1,12 ==> 1; 66,68,1 ==>68
            String newKey = KVList[0];
            if (KVList.length > 3) {
                for (int i = 1; i < KVList.length-2; i++) {
                    newKey = newKey + "," + KVList[i];
                } // 100,200,1,12 ==> 100,200
            }
            // System.out.println("++++++++++++");
            // System.out.println(newKey +", "+ newVal);
            // System.out.println("------------");
            context.write(new Text(newKey), new Text(newVal));
        }
      }
    }


    public static class ReducerKCand
      extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException
        { 
            ArrayList<String> allValues = new ArrayList<String>();
            for (Text Val:values) {
                allValues.add(Val.toString());
            }
            
            if (allValues.size() > 1) {
                // sort the arraylist
                Collections.sort(allValues);
                // concatenate new pairs

                for (int i=0; i<allValues.size(); i++) {
                    for (int j=i+1; j<allValues.size(); j++) {
                        String newPair = allValues.get(i) + "," + allValues.get(j);
                        if (key.toString().equals("#")) {
                            if (!Candidates.contains(newPair)) {
                                Candidates.add(newPair);
                                context.write(new Text(newPair), new IntWritable(1));
                            }
                            
                        } else {
                            String newPair2 = key.toString()  + "," + newPair;
                            if (!Candidates.contains(newPair2)) {
                                Candidates.add(newPair2);
                                context.write(new Text(newPair2), new IntWritable(1));
                            }
                        }

                    }
                }

            }
        }
    }




    public static class MapperKFilter
    extends Mapper<LongWritable, Text, Text, IntWritable>{
      public void map(LongWritable key, Text value, Context context
      ) throws IOException, InterruptedException {
        String val = value.toString();
        int idx = val.indexOf(",");
        String ids = val.substring(idx+1, val.length());
        String[] id_list = ids.split(",");
        for (String pair:Candidates){
         // check if the pair in the id_list
         boolean contain = true;
         String[] component = pair.split(",");
         for (int i=0; i<component.length; i++) {
            String comp = component[i];
            for (int j=0; j<id_list.length; j++){
                if (comp.equals(id_list[j])) {
                    break;
                }
                if ((!comp.equals(id_list[j])) && (j == id_list.length-1)) {
                    contain = false;
                }
            }
            if (! contain) {
                break;
            }
        }

         if (contain) {
            // System.out.println("==============");
            //  System.out.println(pair);
             context.write(new Text(pair), new IntWritable(1));
         }
        }
      }
    }

    public static class ReducerKFilter
    extends Reducer<Text, IntWritable, Text, IntWritable> {
      public void reduce(Text key, Iterable<IntWritable> values,
          Context context) throws IOException, InterruptedException
      { 
        int count = 0;
        for (IntWritable val:values) {
            count += 1;
        }
        int s = Integer.parseInt(support);
        if (count >= s) {
            context.write(key, new IntWritable(count));
        }
      }
    }
    



    public static class MapperConf
    extends Mapper<LongWritable, Text, Text, IntWritable>{
      public void map(LongWritable key, Text value, Context context
      ) throws IOException, InterruptedException {
        // only consider K=2
        // String[] allParts = value.toString().split(",");
        // context.write(new Text(allParts[0] + "," + allParts[1]), new IntWritable(Integer.parseInt(allParts[2])));
        // context.write(new Text(allParts[1] + "," + allParts[0]), new IntWritable(Integer.parseInt(allParts[2])));
        context.write(value, new IntWritable(1));    
    }
      }
    
    public static class ReducerConf
    extends Reducer<Text, IntWritable, Text, Text> {
      public void reduce(Text key, Iterable<IntWritable> values,
          Context context) throws IOException, InterruptedException
      { 
        //   double numer = 0.0;
        //   for (IntWritable val:values) {
        //       numer = Double.parseDouble(val.toString());
        //   }
        //   int deno = Xcount.get(key.toString().split(",")[0]);
        //   double newVal = numer/deno;
        //   String newKey = key.toString().replace(",", "->");
        //   context.write(new Text(newKey), new Text(Double.toString(newVal)));

        String[] Vals = key.toString().split(",");
        String denoKey = Vals[0];
        Double numer = Double.parseDouble(Vals[Vals.length-1]);
        int deno = Xcount.get(denoKey);
        double conf = numer/deno;
        String newKey = Vals[0] + "->" + Vals[1];
        context.write(new Text(newKey), new Text(Double.toString(conf)));
        
        
        String denoKey1 = Vals[1];
        Double numer1 = Double.parseDouble(Vals[Vals.length-1]);
        int deno1 = Xcount.get(denoKey1);
        double conf1 = numer1/deno1;
        String newKey1 = Vals[1] + "->" + Vals[0];
        context.write(new Text(newKey1), new Text(Double.toString(conf1)));
      }
    }


    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        for(int i = 0; i < args.length; ++i) {
            if (args[i].equals("-k")) {
                K =  args[++i];
            } else if (args[i].equals("-s")) {
                support =  args[++i];
            } else if (args[i].equals("--ratingsFile")) {
                inputRatings = args[++i];
            } else if (args[i].equals("--outputScheme")) {
                outputPath = args[++i];
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }
      
          if (K==null || support==null || outputPath == null || inputRatings == null) {
              throw new RuntimeException("Either outputpath or input path are not defined");
          }
        int intK = Integer.parseInt(K);
        
        for (int i=0; i<intK; i++) {
            // clean up candidates
            Candidates = new ArrayList();
            if (i==0) {
                // read from original dataset
                Configuration conf1 = new Configuration();
                conf1.set("mapred.textoutputformat.separator", ",");
                conf1.set("mapreduce.job.queuename", "eecs476w21");         
              
                Job Job1  = Job.getInstance(conf1, "Job1");
                Job1.setJarByClass(AssociationRules.class);
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
            }
            else {
                // generate candidates
                Configuration confi = new Configuration();
                confi.set("mapred.textoutputformat.separator", ",");
                confi.set("mapreduce.job.queuename", "eecs476w21");         
              
                Job Jobi  = Job.getInstance(confi, "Jobi");
                Jobi.setJarByClass(AssociationRules.class);
                Jobi.setNumReduceTasks(1);
                Jobi.setMapperClass(MapperKCand.class);
                Jobi.setReducerClass(ReducerKCand.class);
                Jobi.setMapOutputKeyClass(Text.class);
                Jobi.setMapOutputValueClass(Text.class);
                Jobi.setOutputKeyClass(Text.class);
                Jobi.setOutputValueClass(IntWritable.class);
                FileInputFormat.addInputPath(Jobi, new Path(outputPath+Integer.toString(i)+"/part-r-00000"));
                FileOutputFormat.setOutputPath(Jobi, new Path(outputPath + Integer.toString(-1*i)));
                Jobi.waitForCompletion(true);
                // scan and filter
                Configuration confj = new Configuration();
                confj.set("mapred.textoutputformat.separator", ",");
                confj.set("mapreduce.job.queuename", "eecs476w21");         
              
                Job Jobj  = Job.getInstance(confj, "Jobj");
                Jobj.setJarByClass(AssociationRules.class);
                Jobj.setNumReduceTasks(1);
                Jobj.setMapperClass(MapperKFilter.class);
                Jobj.setReducerClass(ReducerKFilter.class);
                Jobj.setMapOutputKeyClass(Text.class);
                Jobj.setMapOutputValueClass(IntWritable.class);
                Jobj.setOutputKeyClass(Text.class);
                Jobj.setOutputValueClass(IntWritable.class);
                FileInputFormat.addInputPath(Jobj, new Path(inputRatings));
                FileOutputFormat.setOutputPath(Jobj, new Path(outputPath + Integer.toString(i+1)));
                Jobj.waitForCompletion(true);

            }

        }
 

        // Association rules
        Xcount = new HashMap<String, Integer>();
        // read keys from the last output
        String filePath = outputPath + Integer.toString(intK) + "/part-r-00000";
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String line;
        
        while ((line = reader.readLine()) != null) {
            Xcount.put(line.split(",")[0], Integer.MAX_VALUE);
            Xcount.put(line.split(",")[1], Integer.MAX_VALUE);
        }
        
        // read values from the first output
        String filePath2 = outputPath + "1" + "/part-r-00000";
        BufferedReader reader2 = new BufferedReader(new FileReader(filePath2));
        String line2;
        while ((line2 = reader2.readLine()) != null) {
            String key_ = line2.split(",")[0];
            String val_ = line2.split(",")[1];
            if (Xcount.containsKey(key_)) {
                Xcount.put(key_,Integer.parseInt(val_));
            }
        }
        System.out.println(Xcount);
        // MapReduce to calculate confidence
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("mapreduce.job.queuename", "eecs476w21");         
      
        Job JobC  = Job.getInstance(conf, "JobC");
        JobC.setJarByClass(AssociationRules.class);
        JobC.setNumReduceTasks(1);
        JobC.setMapperClass(MapperConf.class);
        JobC.setReducerClass(ReducerConf.class);
        JobC.setMapOutputKeyClass(Text.class);
        JobC.setMapOutputValueClass(IntWritable.class);
        JobC.setOutputKeyClass(Text.class);
        JobC.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(JobC, new Path(filePath));
        FileOutputFormat.setOutputPath(JobC, new Path(outputPath + Integer.toString(intK+1)));
        JobC.waitForCompletion(true);

    }
}
