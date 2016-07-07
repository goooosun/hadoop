package novelsAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;


import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.nlpcn.commons.lang.util.IOUtil;


public class NovelsAnalysis {
		
		 public static java.util.List<String> novelsNameList=new ArrayList<String>();
		 public static java.util.List<String> everyList=new ArrayList<String>();
		 
		 //判断名字是否存在于金庸小说
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
		 
		 
		 //判断每段名字是否重复
		 public static boolean nameIsRepeated(String name)
		 {
			 int i=0;
			 for(;i<everyList.size();i++){
			   if(everyList.get(i).equals(name)){
				   return true;
			   }
			 }
			// if(i==everyList.size())
				 return false;
		 }
		 
		 
		 //添加词典，这个词典是本地文件
		 public static void add_dict(String path) throws IOException{
			 BufferedReader reader=IOUtil.getReader(path, "UTF-8");
			 String temp=null;
			 while((temp=reader.readLine())!=null){
				 novelsNameList.add(temp);
				 UserDefineLibrary.insertWord(temp, "nr", 1000);
			 }
			 
		 }
		 
		 
			//姓名分割与提取
		   public static class nameSplitMapper extends Mapper<Object,Text,Text,Text>{
			   
			   //分布式系统的听词表读取
		/*	   private Path[] localFiles;
			   public void setup(Context context)throws IOException,InterruptedException{
				   Configuration configuration=context.getConfiguration();
				   localFiles=DistributedCache.getLocalCacheFiles(configuration);
				   
				   for(int i=0;i<localFiles.length;i++){
					   String line;
					   BufferedReader reader=new BufferedReader(new FileReader(localFiles[i].toString()));
					   while((line=reader.readLine())!=null){
							 novelsNameList.add(line);
							 UserDefineLibrary.insertWord(line, "nr", 1000);
						 }
				   }
				   
			   }
			   */
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
		 
		   public static class nameSplitReducer extends Reducer<Text, Text, Text, Text>
		   {
			   public void reduce(Text key,Text values,Context context)throws IOException,InterruptedException 
			   {
				
				   context.write(key, values);
			   }
		   }

		   
		   //删除每行中重复的姓名，建立同现关系
		   public static class secondMapper extends Mapper<LongWritable, Text, Text, Text> {
			   
			    Text newKey=new Text();
			    Text newValue=new Text();
			   
			   public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
				  
				   everyList.clear();
				   
				   StringTokenizer str=new StringTokenizer(value.toString());
				   
				   while(str.hasMoreTokens())
				   {
					   String nextTokenString=str.nextToken();
					   if(!nameIsRepeated(nextTokenString))
					    { 
						   everyList.add(nextTokenString);
					    }
				   }
			       for(int i=0;i<everyList.size();i++)
			       {
			    	   for(int j=0;j<everyList.size();j++)
			    	   {
			    		   if(j==i)
			    			   continue;
			    		   else{
			    			   newKey.set(everyList.get(i)+","+everyList.get(j));
			    			   newValue.set("1");
			    			   context.write(newKey, newValue);
			    		   }
			    	   }
			       }
				    
			   }   
		   }
		  
		   //同现关系计数，获得同现总次数
		   public static class secondReducer extends Reducer<Text, Text, Text, Text>{
			   public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
		         
				   Text newKey=new Text();
			       Text newValue=new Text();
			       
			       //统计词频
		            int sum=0;
		            for (Text value:values) {
		                sum+=Integer.parseInt(value.toString());
		            }
		            
		            newValue.set(String.valueOf(sum));
		            
		            context.write(key, newValue);
			   }
		   }
		   
		   
		   //统计出的人物共现次数结果，转换成邻接表的形式表示
		   public static class thirdMapper extends Mapper<LongWritable, Text, Text, Text>
		   {
			   private static Text newKey=new Text();
			   private static Text newValue=new Text();
			   
			   public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException {
				  
				   
				   StringTokenizer str=new StringTokenizer(value.toString());
			       String[] tokens = str.nextToken().toString().split(",");
			       
			       String a = tokens[0];
			       String b = tokens[1];
				   newKey.set(a+"");
				   newValue.set(b+" "+str.nextToken());
				   context.write(newKey, newValue);
			   }
		   }
		   
		   public static class thirdReducer extends Reducer<Text, Text, Text, Text>{
			  
			   
			   Map<String,Integer> mapAll=new HashMap<String,Integer>(); 
			   
			  
			   private static Text newValue=new Text();
			   public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
				  
				   mapAll.clear();
				   double number=0.0;
				   for(Text value:values)
				   {
					   StringTokenizer str=new StringTokenizer(value.toString());
					   String firString=str.nextToken();
					   String secString=str.nextToken();
					   mapAll.put(firString, Integer.parseInt(secString));
					   number+=Integer.parseInt(secString);
				   }
				   String valueString="";
				   if(number!=0.0){
				   for(Map.Entry<String, Integer> entry:mapAll.entrySet()){    
					   valueString+=entry.getKey()+","+entry.getValue()/number+"|";
					}
				   newValue.set(valueString);
				   context.write(key, newValue);
				   }
				 
			   }
		   }
		   
		   
		   
		   //PageRank计算
		   public static class firstPRIterMapper extends Mapper<LongWritable,Text,Text,Text>{
			   public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{
		           double pr = 1.0;
		           Text newkey=new Text();
				   String line = value.toString();
				   StringTokenizer itr=new StringTokenizer(line);
				   newkey.set(itr.nextToken());
				   line=itr.nextToken();
				   String[] tuples = line.split("\\|");
				   if(tuples.length > 0){
					   for(String tuple : tuples){
						   String[] temp = tuple.split(",");
						   String prValue = newkey.toString() + "\t" + String.valueOf(pr * Double.parseDouble(temp[1]));
						   context.write(new Text(temp[0]), new Text(prValue));
					   }
					   context.write(newkey, new Text("!" + line));
				   }
			   }
		   }
			
		   public static class PRIterMapper extends Mapper<LongWritable,Text,Text,Text>{
			   public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{
				   String line = value.toString();
				   String[] tuple = line.split("\t");
				   String name = tuple[0];
				   double pr = Double.parseDouble(tuple[1]);
				   if(tuple.length > 2){
					   String[] linkNames = tuple[2].split("\\|");
					   for(String linkName : linkNames){
					       String[] temp = linkName.split(",");
						   String prValue = name + "\t" + String.valueOf(pr * Double.parseDouble(temp[1]));
						   context.write(new Text(temp[0]), new Text(prValue));
					   }
					   context.write(new Text(name), new Text("!" + tuple[2]));
				   }
			   }
		   }
		   
		   public static class PRIterReducer extends Reducer<Text, Text, Text, Text>
		   {
			   public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
			      String links = "";
			      double damping = 0.85;
			      double pr = 0;
			      for (Text value : values){
			    	  String temp = value.toString();
			    	  if(temp.startsWith("!")){
			    		  links = "\t" + temp.substring(temp.indexOf("!") + 1);
			    		  continue;
			    	  }
			    	  String[] tuple = temp.split("\t");
			    	  if(tuple.length > 1)
			    		  pr += Double.parseDouble(tuple[1]);
			      }
			      pr = (double)(1 - damping) + damping * pr;
			      context.write(new Text(key), new Text(String.valueOf(pr) + links));
			   }
		   }
		  
		   public static class PRViewerMapper extends Mapper<LongWritable, Text, Text, Text> {
			   Text outpr = new Text();
			   Text newvalue=new Text();
			   public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			      String[] line = value.toString().split("\t");
			      float pr = Float.parseFloat(line[1]);
			      outpr.set(String.valueOf(pr));
			      newvalue.set(line[0]);
			      context.write(outpr, newvalue);
			   }   
		   }

		   
		   public static class PRViewerReducer extends Reducer<Text, Text, Text, Text>
		   {
			   public void reduce(Text key,Text values,Context context)throws IOException,InterruptedException{
			     
			      context.write(key, values);
			   }
		   }
		   
		   
		   //驱动程序
		   public static void main(String[] args)throws Exception {
			   
			   Configuration conf = new Configuration();
			   //添加词典
			   add_dict("/home/hadoop/workspace/NovelsAnalysis/data/people_name_list.txt");
			 //  DistributedCache.addCacheFile(new URI("hdfs://114.212.190.91:50070/data/task2/people_name_list.txt"), conf);
			   
			  // DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/user/hadoop/in/people_name_list.txt"), conf);
			   
			   //获取输入输出目录
			   String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
		       if (otherArgs.length!=2) {
		           System.err.println("Word_split<in><out>");
		           System.exit(2);
		       }
				//删除输出目录
		        String[] outTempStrings={"out_temp1","out_temp2","out_temp3","out_temp4"};
		       
				Path outputPath = new Path(args[1]);
				outputPath.getFileSystem(conf).delete(outputPath, true);
			
				
				Path outputPath1 = new Path(outTempStrings[0]);
				outputPath.getFileSystem(conf).delete(outputPath1, true);
				Path outputPath2 = new Path(outTempStrings[1]);
				outputPath.getFileSystem(conf).delete(outputPath2, true);
				Path outputPath3 = new Path(outTempStrings[2]);
				outputPath.getFileSystem(conf).delete(outputPath3, true);
				Path outputPath4 = new Path(outTempStrings[3]);
				outputPath.getFileSystem(conf).delete(outputPath4, true);

				//分割名字并进行过滤
				Job firstjob=new Job(conf,"name_split");
				firstjob.setJarByClass(NovelsAnalysis.class);
				firstjob.setMapperClass(nameSplitMapper.class);
				firstjob.setMapOutputKeyClass(Text.class);
				firstjob.setMapOutputValueClass(Text.class);
				firstjob.setReducerClass(nameSplitReducer.class);
				firstjob.setOutputKeyClass(Text.class);
				firstjob.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(firstjob, new Path(otherArgs[0]));
		        FileOutputFormat.setOutputPath(firstjob, new Path(outTempStrings[0]));
		        firstjob.waitForCompletion(true);
		        		
		        
		        
		        //secondJob
		        Job secondjob=new Job(conf,"secondjob");
		        secondjob.setJarByClass(NovelsAnalysis.class);
		        secondjob.setMapperClass(secondMapper.class);
		        secondjob.setMapOutputKeyClass(Text.class);
		        secondjob.setMapOutputValueClass(Text.class);
		        secondjob.setReducerClass(secondReducer.class);
		        secondjob.setOutputKeyClass(Text.class);
		        secondjob.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(secondjob, new Path(outTempStrings[0]));
		        FileOutputFormat.setOutputPath(secondjob, new Path(outTempStrings[1]));
		        
		        secondjob.waitForCompletion(firstjob.isComplete());
		        
		        
		      //thirdJob
		        Job thirdjob=new Job(conf,"thirdjob");
		        thirdjob.setJarByClass(NovelsAnalysis.class);
		        thirdjob.setMapperClass(thirdMapper.class);
		        thirdjob.setMapOutputKeyClass(Text.class);
		        thirdjob.setMapOutputValueClass(Text.class);
		        thirdjob.setReducerClass(thirdReducer.class);
		        thirdjob.setOutputKeyClass(Text.class);
		        thirdjob.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(thirdjob, new Path(outTempStrings[1]));
		        FileOutputFormat.setOutputPath(thirdjob, new Path(outTempStrings[2]));
		        
		        thirdjob.waitForCompletion(secondjob.isComplete());
		        
		        
		        Job jobIter1=new Job(conf,"jobIter1");
				jobIter1.setJarByClass(NovelsAnalysis.class);
				jobIter1.setMapperClass(firstPRIterMapper.class);
				jobIter1.setMapOutputKeyClass(Text.class);
				jobIter1.setMapOutputValueClass(Text.class);
				jobIter1.setReducerClass(PRIterReducer.class);
			    jobIter1.setOutputKeyClass(Text.class);
				jobIter1.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(jobIter1, new Path(outTempStrings[2]));
		        FileOutputFormat.setOutputPath(jobIter1, new Path(outTempStrings[3]+"/Data1"));
		        jobIter1.waitForCompletion(thirdjob.isComplete());
		        
		        Job jobIter2=new Job(conf,"jobIter2");
				jobIter2.setJarByClass(NovelsAnalysis.class);
				jobIter2.setMapperClass(PRIterMapper.class);
				jobIter2.setMapOutputKeyClass(Text.class);
				jobIter2.setMapOutputValueClass(Text.class);
				jobIter2.setReducerClass(PRIterReducer.class);
			    jobIter2.setOutputKeyClass(Text.class);
				jobIter2.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(jobIter2, new Path(outTempStrings[3]+"/Data1"));
		        FileOutputFormat.setOutputPath(jobIter2, new Path(outTempStrings[3]+"/Data2"));
		        jobIter2.waitForCompletion(jobIter1.isComplete());
		        
		        Job jobIter3=new Job(conf,"jobIter3");
				jobIter3.setJarByClass(NovelsAnalysis.class);
				jobIter3.setMapperClass(PRIterMapper.class);
				jobIter3.setMapOutputKeyClass(Text.class);
				jobIter3.setMapOutputValueClass(Text.class);
				jobIter3.setReducerClass(PRIterReducer.class);
			    jobIter3.setOutputKeyClass(Text.class);
				jobIter3.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(jobIter3, new Path(outTempStrings[3]+"/Data2"));
		        FileOutputFormat.setOutputPath(jobIter3, new Path(outTempStrings[3]+"/Data3"));
		        jobIter3.waitForCompletion(jobIter2.isComplete());
		        
		        Job jobIter4=new Job(conf,"jobIter4");
				jobIter4.setJarByClass(NovelsAnalysis.class);
				jobIter4.setMapperClass(PRIterMapper.class);
				jobIter4.setMapOutputKeyClass(Text.class);
				jobIter4.setMapOutputValueClass(Text.class);
				jobIter4.setReducerClass(PRIterReducer.class);
			    jobIter4.setOutputKeyClass(Text.class);
				jobIter4.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(jobIter4, new Path(outTempStrings[3]+"/Data3"));
		        FileOutputFormat.setOutputPath(jobIter4, new Path(outTempStrings[3]+"/Data4"));
		        jobIter4.waitForCompletion(jobIter3.isComplete());
		        
		        Job jobIter5=new Job(conf,"jobIter5");
				jobIter5.setJarByClass(NovelsAnalysis.class);
				jobIter5.setMapperClass(PRIterMapper.class);
				jobIter5.setMapOutputKeyClass(Text.class);
				jobIter5.setMapOutputValueClass(Text.class);
				jobIter5.setReducerClass(PRIterReducer.class);
			    jobIter5.setOutputKeyClass(Text.class);
				jobIter5.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(jobIter5, new Path(outTempStrings[3]+"/Data4"));
		        FileOutputFormat.setOutputPath(jobIter5, new Path(outTempStrings[3]+"/Data5"));
		        jobIter5.waitForCompletion(jobIter4.isComplete());
		        
		        Job jobIter6=new Job(conf,"jobIter6");
				jobIter6.setJarByClass(NovelsAnalysis.class);
				jobIter6.setMapperClass(PRIterMapper.class);
				jobIter6.setMapOutputKeyClass(Text.class);
				jobIter6.setMapOutputValueClass(Text.class);
				jobIter6.setReducerClass(PRIterReducer.class);
			    jobIter6.setOutputKeyClass(Text.class);
				jobIter6.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(jobIter6, new Path(outTempStrings[3]+"/Data5"));
		        FileOutputFormat.setOutputPath(jobIter6, new Path(outTempStrings[3]+"/Data6"));
		        jobIter6.waitForCompletion(jobIter5.isComplete());
		        
		        Job jobIter7=new Job(conf,"jobIter7");
				jobIter7.setJarByClass(NovelsAnalysis.class);
				jobIter7.setMapperClass(PRIterMapper.class);
				jobIter7.setMapOutputKeyClass(Text.class);
				jobIter7.setMapOutputValueClass(Text.class);
				jobIter7.setReducerClass(PRIterReducer.class);
			    jobIter7.setOutputKeyClass(Text.class);
				jobIter7.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(jobIter7, new Path(outTempStrings[3]+"/Data6"));
		        FileOutputFormat.setOutputPath(jobIter7, new Path(outTempStrings[3]+"/Data7"));
		        jobIter7.waitForCompletion(jobIter6.isComplete());
		        
		        Job jobIter8=new Job(conf,"jobIter8");
				jobIter8.setJarByClass(NovelsAnalysis.class);
				jobIter8.setMapperClass(PRIterMapper.class);
				jobIter8.setMapOutputKeyClass(Text.class);
				jobIter8.setMapOutputValueClass(Text.class);
				jobIter8.setReducerClass(PRIterReducer.class);
			    jobIter8.setOutputKeyClass(Text.class);
				jobIter8.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(jobIter8, new Path(outTempStrings[3]+"/Data7"));
		        FileOutputFormat.setOutputPath(jobIter8, new Path(outTempStrings[3]+"/Data8"));
		        jobIter8.waitForCompletion(jobIter7.isComplete());
		        
		        Job jobIter9=new Job(conf,"jobIter9");
				jobIter9.setJarByClass(NovelsAnalysis.class);
				jobIter9.setMapperClass(PRIterMapper.class);
				jobIter9.setMapOutputKeyClass(Text.class);
				jobIter9.setMapOutputValueClass(Text.class);
				jobIter9.setReducerClass(PRIterReducer.class);
			    jobIter9.setOutputKeyClass(Text.class);
				jobIter9.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(jobIter9, new Path(outTempStrings[3]+"/Data8"));
		        FileOutputFormat.setOutputPath(jobIter9, new Path(outTempStrings[3]+"/Data9"));
		        jobIter9.waitForCompletion(jobIter8.isComplete());
		        
		        Job jobIter10=new Job(conf,"jobIter10");
				jobIter10.setJarByClass(NovelsAnalysis.class);
				jobIter10.setMapperClass(PRIterMapper.class);
				jobIter10.setMapOutputKeyClass(Text.class);
				jobIter10.setMapOutputValueClass(Text.class);
				jobIter10.setReducerClass(PRIterReducer.class);
			    jobIter10.setOutputKeyClass(Text.class);
				jobIter10.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(jobIter10, new Path(outTempStrings[3]+"/Data9"));
		        FileOutputFormat.setOutputPath(jobIter10, new Path(outTempStrings[3]+"/Data10"));
		        jobIter10.waitForCompletion(jobIter9.isComplete());
		        
		        Job jobViewer=new Job(conf,"jobViewer");
		        jobViewer.setJarByClass(NovelsAnalysis.class);
		        jobViewer.setMapperClass(PRViewerMapper.class);
		        jobViewer.setMapOutputKeyClass(Text.class);
		        jobViewer.setMapOutputValueClass(Text.class);
		        jobViewer.setReducerClass(PRViewerReducer.class);
		        jobViewer.setOutputKeyClass(Text.class);
		        jobViewer.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(jobViewer, new Path(outTempStrings[3]+"/Data10"));
		        FileOutputFormat.setOutputPath(jobViewer, new Path(otherArgs[1]));
		        jobViewer.waitForCompletion(jobIter10.isComplete());
		}
	 }
