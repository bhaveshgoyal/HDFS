import java.rmi.RMISecurityManager;
import java.rmi.Naming;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import com.bagl.protobuf.Hdfs.*;
import java.io.*;
public class DataNode implements IDataNode
{	
	static String host = "54.254.144.108";
	static Registry registry;
	static int port = 1099;
    static int block_num;
	static INameNode namenode;
	static int HB_TIME = 2000;
	static int DN_ID;
	
	public static class BlockReport extends Thread
	 {
	 public BlockReport(){}
	 public void run() 
	 {
		try{
			 DataNodeLocation.Builder dnl = DataNodeLocation.newBuilder();
			 dnl.setIp(host);
			 dnl.setPort(port);
			 while(true){
			 BlockReportRequest.Builder brr = BlockReportRequest.newBuilder();
			 brr.setId(DN_ID);	
			 brr.setLocation(dnl.build());			
                         FileReader fileReader = new FileReader("BlockReport");
                         BufferedReader bufferedReader = new BufferedReader(fileReader);
                         StringBuffer stringBuffer = new StringBuffer();
                         String line;
                         line = bufferedReader.readLine();
                         while(line!=null)
                         {
                                System.out.println(line);
                                String[] temp;
                                temp = line.split(" ");
				brr.addBlockNumbers(Integer.parseInt(temp[0]));
                                line = bufferedReader.readLine();
                         }
			 BlockReportResponse brr_response = BlockReportResponse.parseFrom(namenode.blockReport(brr.build().toByteArray()));
			 if(brr_response.getStatus(0) < 0)
			 {
				System.out.println("Error: Could not Send Block Report to NameNode");
			 }
			 else{
				System.out.println("Block Report Sent Successfully to NameNode");	
			 }
			 Thread.sleep(5000);
			 }
		}
		catch (Exception e)
		{
			System.out.println("Error: Something went bad while sending Block Report: " + e.getMessage());
		}
	}
	}
    byte[] writeBlock(byte[] inp) throws RemoteException{
    WriteBlockResponse resp = null;
    try{
            File dir = new File("Blocks");
            WriteBlockRequest write_req = Hdfs.WriteBlockRequest.parseFrom(inp);


            block_num = write_req.getBlockInfo().getBlockNumber();
            File blockFile = new File(dir, String.valueOf(blockNum));
            FileOutputStream fos = new FileOutputStream(blockFile);

            List<ByteString> dataString = writeBlockRequest.getDataList();
            for(ByteString byteString : dataString)
                fos.write(byteString.toByteArray());

            fos.close();

            File report = new File("BlockReport");
            FileWriter fw = new FileWriter(report.getName(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(Integer.toString(blockNum));
            bw.newLine();
            bw.close();
            return resp.setStatus(1).build().toByteArray();
            
        } catch( Exception e) {
            System.out.println("Error: Could not write recieved Block" + e.getMessage());
            resp.setStatus(-1);
            e.printStackTrace();
        }

        return resp.build().toByteArray(); 
    }
    
    
	public static void main(String args[]){
		DN_ID  = Integer.parseInt(args[0]);		
		try{
			System.out.println("DataNode " + DN_ID + " ready...");
			registry = LocateRegistry.getRegistry(host, port);
			final String[] names = registry.list();
			namenode = (INameNode)Naming.lookup("//" + host + "/" + "NameNode");

			Thread Heartbeat = new Thread(new Runnable(){
				
				@Override
				public void run(){
					while(true){
					HeartBeatRequest.Builder hb = HeartBeatRequest.newBuilder().setId(DN_ID);
					try{
                       	 		HeartBeatRequest array = hb.build();
                       			HeartBeatResponse hb_response = HeartBeatResponse.parseFrom(namenode.heartBeat(array.toByteArray()));
					System.out.println("Heartbeat acknowleged by NameNode with status: " + hb_response.getStatus());
					Thread.sleep(HB_TIME);
					}
					catch (Exception e){
						System.out.println("Heartbeat Thread Exception encountered: " + e.getMessage());
						e.printStackTrace();
					}
					}
				}

			
			});
			Heartbeat.start();
			BlockReport b_report = new BlockReport();
			b_report.start();
		}
		catch (Exception e){
			System.out.println("DNerr" + e.getMessage());
			e.printStackTrace();
		}

	}

}
