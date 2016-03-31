import java.rmi.Naming;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import com.bagl.protobuf.Hdfs.*;
import java.io.*;
import java.util.*;

public class NameNode extends UnicastRemoteObject implements INameNode
{	
	static String host = "54.254.144.108";
	static String DN_CONFIG = "./dn.locations";
	static int port = 1099;
	static int f_handle = 1;
	static int BLOCK_NUM = 1;
	private int CASCADE_NUM = 2;
	private HashMap<String, List<String>> dn_map;
	private HashMap<String, Integer> handle_map;
	private HashMap<Integer, ArrayList<Integer>> block_list;
	static Random random_gen;

	public NameNode() throws RemoteException {
		random_gen = new Random();
		handle_map = new HashMap<String, Integer>();
		dn_map = new HashMap<String, List<String>>();
		block_list = new HashMap<Integer, ArrayList<Integer>>();
	}

	public byte[] openFile(byte[] array){
		OpenFileResponse resp = null;
		int handle = -1;
		try {
		BLOCK_NUM = 1;
		OpenFileRequest filerequest = OpenFileRequest.parseFrom(array);
		if (filerequest.getForRead() == false){
			handle = f_handle;
			OpenFileResponse.Builder file_resp = OpenFileResponse.newBuilder();
			file_resp.setHandle(handle);
			handle_map.put(filerequest.getFileName(), handle);
			file_resp.setStatus(1);
			resp = file_resp.build();
			f_handle += 1;
		}
		}
		catch (Exception e){
		System.out.println("Error parsing file Open Request: " + e.getMessage());
		OpenFileResponse.Builder file_resp = OpenFileResponse.newBuilder();
		file_resp.setHandle(handle);
		file_resp.setStatus(-1);
		resp = file_resp.build();
		e.printStackTrace();
		}
		return resp.toByteArray();
		
	}
	public byte[] assignBlock(byte[] array){
		AssignBlockResponse resp = null;
		try{
		AssignBlockRequest assignreq = AssignBlockRequest.parseFrom(array);
		if (assignreq.getHandle() < 0){
			System.out.println("Error: Assigning Block. Invalid File Handle");
			AssignBlockResponse.Builder assignresp = AssignBlockResponse.newBuilder();
			assignresp.setStatus(-1);
			resp = assignresp.build();
		}
		else{
         		File file = new File(DN_CONFIG);
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = bufferedReader.readLine();
			int dnodes_num = 1;
			while (line != null){
				List<String> dn_info = new ArrayList<String>();
				dn_info.add(line.split(" ")[1]);
				dn_info.add(line.split(" ")[2]);
				dn_map.put(line.split(" ")[0], dn_info);
				System.out.println("Found DataNode Bound at: " + ((List)dn_map.get(line.split(" ")[0])).get(0) + ":" + ((List)dn_map.get(line.split(" ")[0])).get(1));
				line = bufferedReader.readLine();
				dnodes_num++;
			}
			dnodes_num--;
			BlockLocations.Builder bloc_build= BlockLocations.newBuilder();
			bloc_build.setBlockNumber(BLOCK_NUM++);
			for(int i=0;i<CASCADE_NUM;i++){
				int idx = random_gen.nextInt(dnodes_num) + 1;
				DataNodeLocation.Builder dnode_build = DataNodeLocation.newBuilder();
				dnode_build.setIp(((List)dn_map.get(idx + "")).get(0) + "");
				dnode_build.setPort(Integer.parseInt(((List)dn_map.get(idx + "")).get(1) + ""));
				bloc_build.addLocations(dnode_build.build());	
			}
			AssignBlockResponse.Builder assignresp = AssignBlockResponse.newBuilder();
			assignresp.setStatus(1);
			assignresp.setNewBlock(bloc_build.build());
			resp = assignresp.build();
			/*List<String> datanodes = new ArrayList<String>();
			for (String entry : boundNames) {
    				if (entry.matches(regex))
      					datanodes.add(entry);
  			}*/
		}
		}
		catch (Exception e){
			AssignBlockResponse.Builder assignresp = AssignBlockResponse.newBuilder();
			assignresp.setStatus(-1);
			resp = assignresp.build();
			System.out.println("Error Assigning Block: " + e.getMessage());
			e.printStackTrace();	
		}
	return resp.toByteArray();	
	}
 	
	public byte[] list(byte array[] ){
                ListFilesResponse.Builder lfr_response = ListFilesResponse.newBuilder();
                try{
                        ListFilesRequest lfr = ListFilesRequest.parseFrom(array);
                        String dir_name = lfr.getDirName();
                        String file_name = dir_name.concat(".txt");
                        lfr_response.setStatus(1);
                        File file = new File(file_name);
                        FileReader fileReader = new FileReader(file);
                        BufferedReader bufferedReader = new BufferedReader(fileReader);
                        StringBuffer stringBuffer = new StringBuffer();
                        String line;
                        line = bufferedReader.readLine();
                        while(line!=null)
                        {
                                System.out.println("Reading Directory.. Found: " + line);
                                String[] temp;
                                temp = line.split(" ");
                                lfr_response.addFileNames(temp[0]);
                                line = bufferedReader.readLine();
                        }
                        return lfr_response.build().toByteArray();
                }
                catch (Exception e){
                        System.out.println("Error: Something went bad while reading the line " + e.getMessage());
                	lfr_response.setStatus(-1);
			e.printStackTrace();
                }
                return lfr_response.build().toByteArray();
        }
	public byte[] getBlockLocations(byte array[])
	{
            BlockLocationResponse.Builder blc_response = null;
            try{
            BlockLocationRequest blc = BlockLocationRequest.parseFrom(array);
            ArrayList<Integer> block_list_temp = new ArrayList<Integer>();
	    for(int block_id: blc.getBlockNumsList()){
                System.out.println("Request recieved for Block Number" + block_id);
                block_list_temp.add(block_id);
	    }
         	blc_response = BlockLocationResponse.newBuilder();
                blc_response.setStatus(1);   
			for(int j=0;j<block_list_temp.size();j++)
			{
				BlockLocations.Builder block = BlockLocations.newBuilder();
                                block.setBlockNumber(block_list_temp.get(j));
				for(int k = 0;k< block_list.get(block_list_temp.get(j)).size();k++)
				{
					ArrayList<Integer> temp = block_list.get(block_list_temp.get(j));
					DataNodeLocation.Builder dnc = DataNodeLocation.newBuilder();
					dnc.setIp(dn_map.get(temp.get(k) + "").get(0));
					dnc.setPort(Integer.parseInt(dn_map.get(temp.get(k) + "").get(1)));
					block.addLocations(dnc.build());
				}
				blc_response.addBlockLocations(block.build());
			}		
			return blc_response.build().toByteArray();
             }	    
            catch (Exception e){
                blc_response.setStatus(-1);   
		System.out.println("Error: Something went wrong while sending Block Locations" + e.getMessage());
                e.printStackTrace();
		System.out.println("something wrong");
	    }
	    return blc_response.build().toByteArray();
	}
	
	public byte[] blockReport(byte array[])
	{	
		
		BlockReportResponse.Builder brr_response = BlockReportResponse.newBuilder();
		try{
			BlockReportRequest brr = BlockReportRequest.parseFrom(array);
 			brr_response.addStatus(1);
			int node_id = brr.getId();
			System.out.println("Block Report Recieved from DataNode: " + node_id + ": " + brr.getLocation().getIp());
			for(int block_id: brr.getBlockNumbersList()){
                		System.out.println(block_id);
 				ArrayList<Integer> temp = new ArrayList<Integer>();
				if(block_list.containsKey(block_id))
				{
					temp = block_list.get(block_id);
				}
				if(!temp.contains(node_id))
				{
					temp.add(node_id);
				}
				block_list.put(block_id,temp);
				
                	}
			if(!dn_map.containsKey(node_id))
			{	
				List<String> dn_info = new ArrayList<String>();
				dn_info.add(brr.getLocation().getIp());
				dn_info.add(brr.getLocation().getPort() + "");
				dn_map.put(node_id + "", dn_info);	
			}
			return brr_response.build().toByteArray();	
		}

  		catch(Exception e)
		{
			brr_response.addStatus(-1);
			System.out.println("Error: Something went Bad while Fetching Block Report " + e.getMessage());
			e.printStackTrace();
		}
		return brr_response.build().toByteArray();
	}
	public byte[] heartBeat(byte[] array) {
	
    	try{
		HeartBeatRequest hb = HeartBeatRequest.parseFrom(array);
	     	int node_num = hb.getId();
             	System.out.println("Recieved HeartBeat from: " + node_num);
                HeartBeatResponse.Builder hb_response = HeartBeatResponse.newBuilder();
                hb_response.setStatus(1);
                HeartBeatResponse array_response = hb_response.build();
                return array_response.toByteArray();
	}
        catch (Exception e)
		{
			System.out.println("Error sending the Heartbeat response");
                	HeartBeatResponse.Builder hb_response = HeartBeatResponse.newBuilder();
                	hb_response.setStatus(0);
                	HeartBeatResponse array_response = hb_response.build();
                	return array_response.toByteArray();
		}
        }
	public static void main(String args[]){
		try{	
			System.out.println("NameNode Server Running running@" + host + "...");
			NameNode obj = new NameNode();
			Naming.rebind("NameNode", obj);
		}
		catch (Exception e){
			System.out.println("NameNode err" + e.getMessage());
			e.printStackTrace();
		}

	}

}
