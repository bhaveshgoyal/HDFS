import java.rmi.Naming;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import com.bagl.protobuf.Hdfs.*;

public class NameNode extends UnicastRemoteObject implements INameNode
{	
	static String host = "54.254.144.108";
	static int port = 1099;
	public NameNode() throws RemoteException {}

	public byte[] heartBeat(byte[] array) {
	
    	try{
		HeartBeatRequest hb = HeartBeatRequest.parseFrom(array);
	     	int node_num = hb.getId();
             	System.out.println("Recieved HeartBeat form: " + node_num);
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
			NameNode obj = new NameNode();
			Naming.rebind("NameNode", obj);
			Naming.rebind("NameNode2", obj);
		}
		catch (Exception e){
			System.out.println("err" + e.getMessage());
			e.printStackTrace();
		}

	}

}
