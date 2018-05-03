package comp9313.ass2;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SingleTargetSP {
	
	// initialize enum
	public static enum MY_COUNTER {
		DISTANCE_UPDATE
	};
	
	/*
	 * SUMMARY
	 * -------
	 * 
	 * There's 3 MapReduce Job that has to be done in order to have the final output for STSP (Single Target Shortest Path)
	 * 
	 * 1st Job: ConvertDataFormat
	 * --------------------------
	 * Convert the input data (EdgeId FromNodeId ToNodeId Distance) into the desired input to be processed.
	 * That is: (KeyNode Initial_Distance_Before_DjikstraAlgorithm AdjacencyList)
	 * KeyNode is the Node
	 * 
	 * Initial_Distance_Before_DjikstraAlgorithm refers to the start distance, that is, if it is the queryNode the distance will be 0, otherwise -1 (means need processing)
	 * 
	 * AdjacencyList will list out all the neighbor nodes of the keyNode
	 * 
	 * Sample Output of 1st job:
	 * -------------------------
	 * Key  0  distAndAdjList 0.0	N|4:2.0 2:5.0
	 * Key  1  distAndAdjList -1.0	N|2:3.0 0:10.0
	 * Key  2  distAndAdjList -1.0	N|1:3.0
	 * Key  3  distAndAdjList -1.0	N|4:6.0 2:9.0 1:1.0
	 * Key  4  distAndAdjList -1.0	N|3:4.0 2:2.0
	 * 
	 * --------------------------------------------------------------------------------------------------------------------------------------------------------------------
	 * 
	 * 2nd Job: STMapper + STReducer (Djikstra Algorithm)
	 * --------------------------------------
	 * This will be the main code for the assignment.
	 * This code will basically run a BFS on the graph, which means it will run iteratively until all node is being traversed.
	 * All the steps (iterative MapReduce run) is stored in the output/tmp/ file which is being coded by the System.NanoTime() so that it cannot contain duplicates which possibly cause error.
	 * The Input will be the same format with what the 1st Job output, that is: (KeyNode Initial_Distance_Before_DjikstraAlgorithm AdjacencyList)
	 * In each run, it will calculate the distance and update minimal distance if possible
	 * Basic Djikstra Algorithm in general.
	 * It will also stores the path when it updates the distance.
	 * 
	 * Sample Output of 2nd Job (1st iteration):
	 * 0	0.0	D|4:2.0 2:5.0
	 * 1	-1.0	D|2:3.0 0:10.0
	 * 2	5.0	N|1:3.0|0
	 * 3	-1.0	D|4:6.0 2:9.0 1:1.0
	 * 4	2.0	N|3:4.0 2:2.0|0
	 * 
	 * This Job will run until all flag (N) changes to (D):
	 * This is what the final Iteration will look like:
	 * 0	0.0	D|4:2.0 2:5.0
	 * 1	7.0	D|2:3.0 0:10.0|2->4->0
	 * 2	4.0	D|1:3.0|4->0
	 * 3	6.0	D|4:6.0 2:9.0 1:1.0|4->0
	 * 4	2.0	D|3:4.0 2:2.0|0
	 * 
	 * --------------------------------------------------------------------------------------------------------------------------------------------------------------------
	 * 
	 * 3rd Job: FinalWriteResult
	 * -------------------------
	 * This Job will take the final iteration of 2nd Job and cleanup, and then output it on output folder with filename format: FinalOutput_QueryNode_<NodeId>
	 * Final Output will the result that the assignment wants:
	 * that is: (Node, MinDistance, Path)
	 * 
	 * Sample Output of 3rd Job:
	 * 0	0.0	0
	 * 1	7.0	1->2->4->0
	 * 2	4.0	2->4->0
	 * 3	6.0	3->4->0
	 * 4	2.0	4->0
	 * 
	 * @author: Peter Leonard, z5128899, z5128899@ad.unsw.edu.au
	 * Feel free to shoot me an email if some codes are not clear. I believe in my ability to explain my codes well, since i construct it myself :)
	 * 
	 */
	
	/*
	 * 1st Job's MapReduce
	 * 
	 * 
	 */
	
	public static void convertDataFormat(String queryNodeId, String rawInput, String input ) throws Exception {
		Configuration conf1 = new Configuration();
		// Passing the queryNodeId to be used in the reducer
        conf1.set("queryNodeId", queryNodeId);
        
        Job job1 = Job.getInstance( conf1, "Convert the input file to the desired format for 2nd Job" );
        job1.setJarByClass(SingleTargetSP.class);
		job1.setMapperClass(ConvertDateFormatMapper.class);
		job1.setReducerClass(ConvertDateFormatReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(rawInput));
		FileOutputFormat.setOutputPath(job1, new Path(input));
		
		job1.waitForCompletion(true);
	}
	
	public static class ConvertDateFormatMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] strings = value.toString().split(" ");
			String toNodeId = strings[2];
			String fromNodeIdAndDist = strings[1] + ":" + strings[3];
			
			context.write(new Text(toNodeId), new Text(fromNodeIdAndDist));
		}
	}
	public static class ConvertDateFormatReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Get the queryNodeId (done by Configuration get/set) to specify(initialize) the dist 
			// of this node is 0.0, while others are all initialized to be -1.0 (inifinite)
			Configuration conf = context.getConfiguration();
		    String queryNodeId = conf.get("queryNodeId");
		    
		    String distAndAdjList = "";
		    if( key.toString().equals(queryNodeId) ){
		    	distAndAdjList += "0.0\tN|";
		    } else {
		    	distAndAdjList += "-1.0\tN|";
		    }
		    
			for (Text value : values) {
				distAndAdjList += value.toString() + " ";
			}
			distAndAdjList = distAndAdjList.trim();
			System.out.println("Key  "+key+"  distAndAdjList "+distAndAdjList);
			context.write( key, new Text(distAndAdjList));
		}
	}
	/*
	 * 1st Job DONE
	 * 
	 */
	
	/*
	 * 2nd Job: run the Dijkstra algorithm iteratively using MapReduce
 	 * The data structure:
	 * keyNode	distance	N/D|Adjacency List<>
	 * status flag "N" means it is updated in the reducer at last round , while "D" means it is unchanged
	 * 
	 * 
	 */
	
    public static class STMapper extends Mapper<Object, Text, LongWritable, Text> {

    	@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Get rid of empty line(s)
			if (value.toString().trim().equals("")){
				return;
			}
			
			String[] strings = value.toString().split("\\|");
			String[] fromNodeIdAndSrcDistAndDupFlag = strings[0].split("\t");

			String fromNodeId = fromNodeIdAndSrcDistAndDupFlag[0];
			Integer fromNodeIdInt = Integer.parseInt(fromNodeId);
			String srcDist = fromNodeIdAndSrcDistAndDupFlag[1];
			Double srcDistDouble = Double.parseDouble(srcDist);
			String dupFlag = fromNodeIdAndSrcDistAndDupFlag[2];

			// The adjacency list could be empty during the iterations
			// for the node that does not has outgoing edges before current iteration
			String[] toNodeIdsAndDist = {};
			if (strings.length != 1) {  
				toNodeIdsAndDist = strings[1].split(" ");
				// Emit the adjacency list to reverse/pass the graph structure
				if (strings.length == 3){
					context.write(new LongWritable(fromNodeIdInt), new Text(srcDist+"\t"+dupFlag+"|"+strings[1]+"|"+strings[2]));
				} else {
					context.write(new LongWritable(fromNodeIdInt), new Text(srcDist+"\t"+dupFlag+"|"+strings[1]));
				}
			} else {
				// If the adjacency list is empty, leave the right side of '|' as empty
				context.write(new LongWritable(fromNodeIdInt), new Text(srcDist+"\t"+dupFlag+"|"));
			}

			// process as long Distance not -1.0 (infinite)
			if (srcDistDouble != -1.0) {
				// If flag is D then skip (since it is already final)
				if (dupFlag.equals("D")){
					return;
				}

				// Emit all related (destNodeId, updatedistForDestNode) pairs.
				// This information is retrieved from Adjacency List read
				for (int i = 0; i < toNodeIdsAndDist.length; i++) {
					String[] fromNodeIdAndToNodeId = toNodeIdsAndDist[i].split(":");
					String toNodeId = fromNodeIdAndToNodeId[0];
					Integer toNodeIdInt = Integer.parseInt(toNodeId);

					String srcDestDist = fromNodeIdAndToNodeId[1];
					Double srcDestDistDouble = Double.parseDouble(srcDestDist);

					// Update the distance by adding the distance of the source node 
					Double updatedDist = srcDistDouble + srcDestDistDouble;
					String updatedDistStr = String.valueOf(updatedDist);
					
					// if it has / on the front that means it is the parent node
					// if it has $ on the front that means it is the ancestor node (parent of parent node)
					// this helps in constructing the Path
					context.write(new LongWritable(toNodeIdInt), new Text("/"+fromNodeId));
					
					// if the format of the input is like:		node, distance, and flag information|Adjacency List| parent
					// that means it has an ancestor (since its parent node has a parent). include it as well
					if (strings.length == 3){
						context.write(new LongWritable(toNodeIdInt), new Text("$"+strings[2]));
					}
					
					// finally emit distance
					context.write(new LongWritable(toNodeIdInt), new Text(updatedDistStr));
				}
			}
		}

    }


    public static class STReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    	@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String origAdjacencyList = "";
			String parent = "";
			String ancestor = "";
			// check all the key value pairs received
			ArrayList<Double> updatedDistances = new ArrayList<Double>();
			for (Text updatedDistOrAdjList : values) {
				String updatedDistOrAdjListStr = updatedDistOrAdjList.toString();		
				// update parents if index 0 contains /
				if (updatedDistOrAdjListStr.indexOf('/') == 0) {
					parent = updatedDistOrAdjListStr.substring(1);
					continue;
				}
				// update ancestor if index 0 contains $
				if (updatedDistOrAdjListStr.indexOf('$') == 0) {
					ancestor = updatedDistOrAdjListStr.substring(1);
					continue;
				}
				
				// other than that only have 2 possibility, updated value, or original adjacency list
				// its adjacency list if it contains |. otherwise, its the updated value (which is the bridge distance from a to b through c)
				if (updatedDistOrAdjListStr.indexOf('|') == -1) {
					// Contains no "|" means it is one of the updated distances
					Double distance = Double.parseDouble(updatedDistOrAdjListStr);
					updatedDistances.add(distance);
				} else {
					// Contains "|" means it is original adjacency list
					origAdjacencyList = updatedDistOrAdjListStr;
				}
			}
			
			// Split the whole string here first to be used in multiple places to improve efficiency.
			String[] strings = origAdjacencyList.split("\\|");
			String origSrcDistStr = "";

			Double currentDistance = -1.0;
			// Just in case the original adjacency list is empty
			if (!origAdjacencyList.equals("")) {
				origSrcDistStr = strings[0].split("\\t")[0];
				currentDistance = Double.parseDouble(origSrcDistStr);
			}
			// Get the minimal distance if it exists
			Double minDistance = Double.POSITIVE_INFINITY;
			for (int i = 0; i < updatedDistances.size(); i++) {
				if (updatedDistances.get(i) < minDistance) {
					minDistance = updatedDistances.get(i);
				}
			}

			// If the updatedMinDistance is smaller than currentMinDistance, than update the whole adjacency list string
			if (minDistance != Double.POSITIVE_INFINITY && (currentDistance == -1.0 || minDistance < currentDistance)) {
				String minDistanceStr = String.valueOf(minDistance);
				// Just in case that origAdjacencyList is "" or "dist|"
				if (strings.length >= 2) {
					origAdjacencyList = minDistanceStr + "\t" + "N" + "|" + strings[1] + "|" + parent;
					if (ancestor != ""){
						origAdjacencyList = origAdjacencyList + "->" + ancestor;
					}
				} else {
					origAdjacencyList = minDistanceStr + "\t" + "N" + "|";
				}
				
				// Update(increment) the counter to indicate the program cannot stop 
				// after this round mapreduce since it is still get updated
				context.getCounter(MY_COUNTER.DISTANCE_UPDATE).increment(1);
			} else {
				// otherwise keep the original info for this source node but set the status flag 
				// to be Duplicated indicate the next round mapper does not need to update the 
				// dist for linking nodes of this source node again, to improve the efficiency
				if (strings.length >= 2) {
					// if it contains ancestor
					if (strings.length == 3){
						origAdjacencyList = origSrcDistStr + "\t" + "D" + "|" + strings[1] + "|" + strings[2];
					} else {
						origAdjacencyList = origSrcDistStr + "\t" + "D" + "|" + strings[1];
					}
				} else {
					origAdjacencyList = origSrcDistStr + "\t" + "D" + "|";
				}
			}
			context.write(key, new Text(origAdjacencyList));
		}
    }
    
    /*
	 * 3rd Job: FinalWriteResult
 	 * The data structure:
	 * keyNodes	Distance	Path
	 * 
	 * This is the final MapReduce jobs for this assignment, it will take the final output temp files, and update it to the wanted output.
	 * 
	 * 
	 */
    
    public static void writeFinalResult(String queryNodeId, String readFolder, String input, String output ) throws Exception {
		Configuration conf3 = new Configuration();
        
        Job job3 = Job.getInstance( conf3, "write the final result" );
        job3.setJarByClass(SingleTargetSP.class);
		job3.setMapperClass(FinalWriteMapper.class);
		job3.setReducerClass(FinalWriteReducer.class);
		
		job3.setOutputKeyClass(NullWritable.class);
		job3.setOutputValueClass(Text.class);
		job3.setMapOutputKeyClass(LongWritable.class);

		// Use the latest output path as the input path to extract and generate the final result 
		FileInputFormat.addInputPath(job3, new Path(readFolder));
		
		// Set the final output path
		String finalOutputPath = output + "/" + "FinalOutput_QueryNode_" + queryNodeId;
		FileOutputFormat.setOutputPath(job3, new Path(finalOutputPath));
		
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}
    
 	public static class FinalWriteMapper extends Mapper<Object, Text, LongWritable, Text> {
 		@Override
 		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
 			// Get rid of empty line(s)
 			if (value.toString().trim().equals("")){
 				return;
 			}
 			String line = value.toString().split("\\|")[0];
 			String[] toNodeIdAndDistArray = line.split("\t");
 			Integer toNodeId = Integer.parseInt(toNodeIdAndDistArray[0]);
 			String dist = toNodeIdAndDistArray[1];
 			// In case the node is unreachable from the source node, then just ignore it
 			if( Double.parseDouble(dist) == -1.0 ){
 				return;
 			}
 			

 			// append the keyNode to the front of the path
 			String path = toNodeIdAndDistArray[0];
 			String[] strings = value.toString().split("\\|");
 			if (strings.length == 3){
 				path += "->" + strings[2];
 			}
 			
 			// Use the "toNodeId" as key to guarantee the targetNodes are sorted in numerical order
 			context.write(new LongWritable(toNodeId), new Text(dist+"\t"+path));
 		}
 	}
 	public static class FinalWriteReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
 		@Override
 		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
 		    String toNodeId = String.valueOf(key.get());
 		    
 			for (Text value : values) {
 				String dist = value.toString().split("\t")[0];
 				String path = value.toString().split("\t")[1];
 				context.write( NullWritable.get(), new Text(toNodeId+"\t"+dist+"\t"+path));
 			}
 		}
 	}


    public static void main(String[] args) throws Exception {        
    	String rawInput = args[0];
        
        String OUT = args[1];
        String output = OUT;

        // Write the intermediate results to HDFS with the path of output+"/tmp/"+systemtime
        // and this would be used as the path of mapper input at next round
        String outputTmp = OUT + "/tmp/" + System.nanoTime();

        String queryNodeId = args[2];

        String input = rawInput+ ".transform";
        
        // Convert the input file to the desired format for iteration using one mapreduce job

	    // YOUR JOB: Convert the input file to the desired format for iteration, i.e., 
        //           create the adjacency list and initialize the distances
        convertDataFormat(queryNodeId, rawInput, input );

        boolean isdone = false;

        while (isdone == false) {
            // YOUR JOB: Configure and run the MapReduce job
    		Configuration conf2 = new Configuration();
    		Job job2 = Job.getInstance(conf2, "find Single-source shortest path");
    		
    		job2.setJarByClass(SingleTargetSP.class);
    		job2.setMapperClass(STMapper.class);
    		job2.setReducerClass(STReducer.class);
    		
    		job2.setOutputKeyClass(LongWritable.class);
    		job2.setOutputValueClass(Text.class);
    		
    		FileInputFormat.addInputPath(job2, new Path(input));
    		FileOutputFormat.setOutputPath(job2, new Path(outputTmp));
    		
    		// Set the number of reducer as 3 for the job running Dijkstra algorithm 
            // job2.setNumReduceTasks(3); 
    		
    		job2.waitForCompletion(true);            
            
    		// Use the current output file as the file to be read by mapper at the next round iteratively...
    		input = outputTmp;        

            //You can consider to delete the output folder in the previous iteration to save disk space.

            // YOUR JOB: Check the termination criterion by utilizing the counter
    		
    		// Check for convergence condition if any node is still left then continue else stop  
            // The best way is to use the counter supported by Hadoop MapReduce. This is the recommended way.
            // We can also read the output into memory to check if all nodes get the distances. However, this is not efficient.
            // If the counter is 0, means all dist are already updated to the shortest distance and could stop
            long count = job2.getCounters().findCounter(MY_COUNTER.DISTANCE_UPDATE).getValue();
    		if( count == 0 ){
    			isdone = true;
    			break;
    		}
    		
    		// New output folder for next round
    		outputTmp = OUT + "/tmp/" + System.nanoTime();
        }

        // YOUR JOB: Extract the final result using another MapReduce job with only 1 reducer, and store the results in HDFS
        
        // Extract the final result and write it to the hdfs file, with the filename
        System.out.println("queryNodeId "+queryNodeId+"  outputTmp  "+outputTmp+" rawInput  "+rawInput+"  output "+output);
        
        writeFinalResult( queryNodeId, outputTmp, rawInput, output );
        // COMPLETE
    }

}



