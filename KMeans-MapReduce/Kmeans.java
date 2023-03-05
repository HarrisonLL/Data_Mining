package com.eecs476;
import java.io.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
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
import java.lang.*;

public class Kmeans {


    public static class Mapper1
        extends Mapper<LongWritable, Text, Text, Text>{

        public ArrayList<ArrayList<Double>> Centriods = new ArrayList<ArrayList<Double>>();

        public void setup(Context context) throws IOException, InterruptedException {
          // read candidates from cache file
          URI[] centroidFiles = context.getCacheFiles();
          // make sure there was at least one file added to cache
          if (centroidFiles != null && centroidFiles.length > 0) {
              // loop over every file in the cache
              int count = 0;
              for (URI centroidFile : centroidFiles) {
                Path inFile = new Path(centroidFile.getPath());
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(inFile)));
                String line;
                while ((line = br.readLine())!= null){
                    String[] lineArr = line.split(",");
                    ArrayList<Double> innerList = new ArrayList<Double>();
                    for (int i = 0; i < lineArr.length; i++) {
                        double elem = Double.parseDouble(lineArr[i]);
                        innerList.add(elem);
                    }
                    Centriods.add(innerList);
                }
                br.close();
                if (count == 0) {break;}
              }
            }
            k = context.getConfiguration().getInt(K_num, -1);
            norm = context.getConfiguration().getInt(Norm_num, -1);
        }

       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            double min_dist = Double.POSITIVE_INFINITY;
            String assigned_cluster = "";
            for (ArrayList l:Centriods) {
                double dist = 0.0;
                String centroid = "";
                if (norm == 1) {
                    for (int i = 0; i < l.size(); i++) {
                        dist += Math.abs(Double.parseDouble(data[i]) - (double)l.get(i));
                        centroid += Double.toString((double)l.get(i)) + ",";
                    }
                } else {
                    for (int i = 0; i < l.size(); i++) {
                        dist += Math.pow(Double.parseDouble(data[i]) - (double)l.get(i), 2);
                        centroid += Double.toString((double)l.get(i)) + ",";
                    }
                }
                centroid = centroid.substring(0, centroid.length() - 1);
                if (dist<min_dist) {
                    min_dist = dist;
                    assigned_cluster = centroid;
                }
            }
            String newValue = value.toString();
            context.write(new Text(assigned_cluster), new Text(newValue));
    }
}

    public static class Reducer1
        extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values,
          Context context) throws IOException, InterruptedException
        { 
            String[] keyList = key.toString().split(",");
            int dim = keyList.length;
            double[] newCenter = new double[dim];
            for (int i = 0; i < dim; i++) {
                newCenter[i] = 0.0;
            }

            int count = 0;
            for (Text val:values) {
                String[] valList = val.toString().split (",");
                for (int i = 0; i < valList.length; i++) {
                    newCenter[i] += Double.parseDouble(valList[i]);
                }
                count += 1;
            
            }
            String newKey = "";
            for (int i = 0; i < dim; i++) {
                newCenter[i] /= count;
                newKey += Double.toString(newCenter[i]) + ",";
            }
            newKey = newKey.substring(0, newKey.length() - 1);
            context.write(new Text(newKey), NullWritable.get());
            
        }
    }
    
    public static final String K_num = "K_NUM";
    public static final String Norm_num = "NORM_NUM";
    public static int k;
    public static int n;
    public static int norm;
    public static String inputPath;
    public static String centroidPath;
    public static String outputScheme;
    
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        for(int i = 0; i < args.length; ++i) {
            if (args[i].equals("--inputPath")) {
                inputPath = args[++i];
            } else if (args[i].equals("--centroidPath")) {
                centroidPath = args[++i];
            } else if (args[i].equals("--outputScheme")) {
                outputScheme = args[++i];
            } else if (args[i].equals("--norm")) {
                norm =  Integer.parseInt(args[++i]);
            } else if (args[i].equals("-k")) {
                k =  Integer.parseInt(args[++i]);
            } else if (args[i].equals("-n")) {
                n =  Integer.parseInt(args[++i]);
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }
      
          if (inputPath == null || centroidPath == null || outputScheme == null) {
              throw new RuntimeException("Either outputpath or input path are not defined");
          }


        for (int i=0; i < n; i++) {
                if (i == 0) {
                // read from original dataset
                Configuration conf1 = new Configuration();
                conf1.set("mapreduce.textoutputformat.separator", ",");
                conf1.set("mapreduce.job.queuename", "eecs476");
                conf1.setInt(K_num, k);
                conf1.setInt(Norm_num, norm);     
              
                Job Job1  = Job.getInstance(conf1, "Job1");
                Job1.setJarByClass(Kmeans.class);
                Job1.setNumReduceTasks(1);
                Job1.setMapperClass(Mapper1.class);
                Job1.setReducerClass(Reducer1.class);
                Job1.setMapOutputKeyClass(Text.class);
                Job1.setMapOutputValueClass(Text.class);
                Job1.setOutputKeyClass(Text.class);
                Job1.setOutputValueClass(NullWritable.class);
                FileInputFormat.addInputPath(Job1, new Path(inputPath));
                FileOutputFormat.setOutputPath(Job1, new Path(outputScheme+Integer.toString(1)));
                // // attach the cache files(candidates) to the context object
                URI[] cachedFilesList = {new Path(centroidPath).toUri()};
                Job1.setCacheFiles(cachedFilesList);
                Job1.waitForCompletion(true);
            } else {
                // read from original dataset
                Configuration confi = new Configuration();
                confi.set("mapreduce.textoutputformat.separator", ",");
                confi.set("mapreduce.job.queuename", "eecs476");
                confi.setInt(K_num, k);
                confi.setInt(Norm_num, norm);     
                
                Job Jobi  = Job.getInstance(confi, "Jobi");
                Jobi.setJarByClass(Kmeans.class);
                Jobi.setNumReduceTasks(1);
                Jobi.setMapperClass(Mapper1.class);
                Jobi.setReducerClass(Reducer1.class);
                Jobi.setMapOutputKeyClass(Text.class);
                Jobi.setMapOutputValueClass(Text.class);
                Jobi.setOutputKeyClass(Text.class);
                Jobi.setOutputValueClass(NullWritable.class);
                FileInputFormat.addInputPath(Jobi, new Path(inputPath));
                FileOutputFormat.setOutputPath(Jobi, new Path(outputScheme+Integer.toString(i+1)));
                URI[] cachedFilesList = {new Path(outputScheme+Integer.toString(i)+ "/part-r-00000").toUri()};
                Jobi.setCacheFiles(cachedFilesList);
                Jobi.waitForCompletion(true);
            }
    
            

        }
    }
}
