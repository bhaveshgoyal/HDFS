import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Random;
import java.rmi.Remote;
import com.bagl.protobuf.Hdfs.*;
import java.io.File;
import java.util.*;
public class TaskTracker extends UnicastRemoteObject
{
	    static String host = "127.0.0.1";
        static int port = 1099;

        static int MAP_CAP = 2;
        static int REDUCE_CAP = 2;
        
        static Registry registry;
	    static IJobTracker jobtracker;
        ExecutorService map_execs, reduce_execs;
        public TaskTracker() throws RemoteException {
        
            map_execs = Executors.newFixedThreadPool(MAP_CAP);
            reduce_execs = Executors.newFixedThreadPool(REDUCE_CAP);
        
        }
        public static void main(String args[]){
                try{
			registry = LocateRegistry.getRegistry(host, port);
			final String[] names = registry.list();
			jobtracker = (IJobTracker)Naming.lookup("//" + host + "/" + "JobTracker");
			new heartBeat().start();
                        
                }
                catch (Exception e){
                        System.out.println("err" + e.getMessage());
                        e.printStackTrace();
                }

        }
        public static class heartBeat extends Thread
	{
		public void heartbeat(){}
		public void run()
		{
			try{
				while(true){
                         	        HeartBeatRequestMapReduce.Builder hbr = HeartBeatRequestMapReduce.newBuilder();
                                   	hbr.setTaskTrackerId(1);
					hbr.setNumMapSlotsFree(MAP_CAP - map_execs.getActiveCount());
					hbr.setNumReduceSlotsFree(REDUCE_CAP - reduce_execs.getActiveCount());
					jobtracker.heartBeat(hbr.build().toByteArray());
					Thread.sleep(1000);
				}
			}
			catch (Exception e)
			{
				System.out.println("Error: Something went bad while sending the Heart Beat: " + e.getMessage());
			}
		}

		
	}
}
