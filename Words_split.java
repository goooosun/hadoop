package words_split;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;


import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.nlpcn.commons.lang.util.IOUtil;


public class Words_split {
	public static String dataHome="/home/hadoop/workspace/NovelsAnalysis/data";
	
	 public static java.util.List<String> novelsNameList=new ArrayList<String>();
	 public static boolean nameIsInNovels(String name)
	 {
		 int i=0;
		 for(;i<novelsNameList.size();i++){
		   if(novelsNameList.get(i).equals(name)){
			   return true;
		   }
		 }
		// if(i==novelsNameList.size())
			 return false;
	 }
	 public static void add_dict(String path) throws IOException{
		 BufferedReader reader=IOUtil.getReader(path, "UTF-8");
		 String temp=null;
		 while((temp=reader.readLine())!=null){
			 novelsNameList.add(temp);
			 UserDefineLibrary.insertWord(temp, "nr", 1000);
		 }
		 
	 }
		//直接传给第一个reduce
	   public static class firstMapper extends Mapper<Object,Text,Text,Text>{
		   private Text newkey=new Text();
		   private Text newValue=new Text();
		   public void map(Object key,Text value,Context context)throws IOException, InterruptedException{
			  // FileSplit fileSplit=(FileSplit)context.getInputSplit();
			  // String fileName=fileSplit.getPath().getName();
			  
			   
			   String newkeyString="";
			   java.util.List<Term> terms=ToAnalysis.parse(value.toString());
			   for(int i=0;i<terms.size();i++){
					if(terms.get(i).getNatureStr().equals("nr")){
						if(nameIsInNovels(terms.get(i).getName())){
							 newkeyString+=terms.get(i).getName().toString();
							 newkeyString+="\t";
							
						}
					}
				}
			 if(newkeyString!=""){
			   newkey.set(newkeyString);
			   newValue.set("");
			   context.write(newkey,newValue);
		     }
		   }
		   
	   }
	   //
	   public static class firstReducer extends Reducer<Text, Text, Text, Text>
	   {
		   public void reduce(Text key,Text values,Context context)throws IOException,InterruptedException 
		   {
			
			   context.write(key, values);
		   }
	   }

	   //驱动程序
	   public static void main(String[] args)throws Exception {
		   //添加词典
		   add_dict(dataHome+"/people_name_list.txt");
		   
		   
		   Configuration conf = new Configuration();
			String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
	       if (otherArgs.length!=2) {
	           System.err.println("Word_split<in><out>");
	           System.exit(2);
	       }
			//删除输出目录
			Path outputPath = new Path(args[1]);
			outputPath.getFileSystem(conf).delete(outputPath, true);
			

			//firstJob
			Job firstjob=new Job(conf,"firstjob");
			firstjob.setJarByClass(Words_split.class);
			firstjob.setMapperClass(firstMapper.class);
			firstjob.setMapOutputKeyClass(Text.class);
			firstjob.setMapOutputValueClass(Text.class);
			firstjob.setReducerClass(firstReducer.class);
			firstjob.setOutputKeyClass(Text.class);
			firstjob.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(firstjob, new Path(otherArgs[0]));
	        FileOutputFormat.setOutputPath(firstjob, new Path(otherArgs[1]));
	        firstjob.waitForCompletion(true);
	        		
	}
 }

