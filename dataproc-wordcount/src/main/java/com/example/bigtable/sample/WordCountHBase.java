
package com.example.bigtable.sample;
import java.io.IOException;
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
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountHBase {
  public static void main(String [] args) throws Exception
  {
    Configuration c=new Configuration();
    String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
    Path input=new Path(files[0]);
    Path output=new Path(files[1]);
    Job j=new Job(c,"wordcount");
    j.setJarByClass(WordCountHBase.class);
    j.setMapperClass(MapForWordCount.class);
    j.setReducerClass(ReduceForWordCount.class);
    j.setOutputKeyClass(Text.class);
    j.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(j, input);
    FileOutputFormat.setOutputPath(j, output);
    System.exit(j.waitForCompletion(true)?0:1);
  }
  //class Map
  public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable>
  {
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
    {
      //lấy thông tin trong file input chuyển sang 1 chuổi kiểu string
      String line = value.toString();
      //tách từng số bỏ vào mảng string
      String[] numbers=line.split(",");
      for(String number: numbers )
      {
        //chuyển sang kiểu số
        int num = Integer.parseInt(number);
        //khởi tạo key out put
        Text outputKey = new Text(number.toUpperCase().trim());
        //khởi tạo biến lưu giá trị của key out put
        IntWritable outputValue = new IntWritable(1);
        //kiểm tra xem số trên phải số nguyên tố hay không
        //nếu đúng thì ghi vào cập giá trị <key,value>
        if(isPrimeNumber(num)){
          con.write(outputKey, outputValue);
        }        
      }
    }
    //hàm kiểm tra số nguyên tố
    public static boolean isPrimeNumber(int n) {
      // so nguyen n < 2 khong phai la so nguyen to
      if (n < 2) {
          return false;
      }
      // check so nguyen to khi n >= 2
      int squareRoot = (int) Math.sqrt(n);
      for (int i = 2; i <= squareRoot; i++) {
          if (n % i == 0) {
              return false;
          }
      }
      return true;
    }

  }
  //class Reduce
  public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable>
  {
    public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
    {
      int sum = 0;
        for(IntWritable value : values)
        {
        sum += value.get();
        }
        con.write(word, new IntWritable(sum));
    }
  }
}


