import java.rmi.RMISecurityManager;
import java.rmi.Naming;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import com.bagl.protobuf.Hdfs.*;
public class DataNode
{	
	static String host = "54.254.144.108";
	static Registry registry;
	static int port = 1099;
	static INameNode namenode;
	static int HB_TIME = 2000;
	static int DN_ID;
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
		}
		catch (Exception e){
			System.out.println("DNerr" + e.getMessage());
			e.printStackTrace();
		}

	}

}
