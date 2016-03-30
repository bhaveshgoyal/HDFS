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
	static INameNode obj;
	static int HB_TIME = 2000;
	public static void main(String args[]){
		
		String msg = "hello";
		
		try{
			System.out.println("hostname set");
			registry = LocateRegistry.getRegistry(host, port);
			final String[] names = registry.list();
			for (int i=0;i<names.length;i++)
				System.out.println(names[i]);
			obj = (INameNode)Naming.lookup("//" + host + "/" + "NameNode");
//			INameNode obj = (INameNode)registry.lookup("NffameNode");
			System.out.println("lookup complete");
			Thread Heartbeat = new Thread(new Runnable(){
				
				@Override
				public void run(){
					while(true){
					HeartBeatRequest.Builder hb = HeartBeatRequest.newBuilder();
                       		 	hb.setId(1);
					try{
                       	 		HeartBeatRequest array = hb.build();
                       			HeartBeatResponse hb_response = HeartBeatResponse.parseFrom(obj.heartBeat(array.toByteArray()));
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
