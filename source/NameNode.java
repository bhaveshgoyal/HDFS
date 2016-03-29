import java.rmi.Naming;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class NameNode extends UnicastRemoteObject implements INameNode
{	
	static String host = "54.254.144.108";
	static int port = 1099;
	public NameNode() throws RemoteException {}

	public String sayHello() {return "Hello World";}

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
