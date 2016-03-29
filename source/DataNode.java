import java.rmi.RMISecurityManager;
import java.rmi.Naming;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;

public class DataNode
{	
	static String host = "54.254.144.108";
	static Registry registry;
	static int port = 1099;

	public static void main(String args[]){
		
		String msg = "hello";
		
		try{
			System.out.println("hostname set");
			registry = LocateRegistry.getRegistry(host, port);
			final String[] names = registry.list();
			for (int i=0;i<names.length;i++)
				System.out.println(names[i]);
			INameNode obj = (INameNode)Naming.lookup("//" + host + "/" + "NameNode");
//			INameNode obj = (INameNode)registry.lookup("NffameNode");
			System.out.println("lookup complete");
			System.out.println(obj.sayHello());
		}
		catch (Exception e){
			System.out.println("DNerr" + e.getMessage());
			e.printStackTrace();
		}

	}

}
