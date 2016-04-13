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
public class JobTracker extends UnicastRemoteObject implements IJobTracker
{
	static String host = "127.0.0.1";
        static int port = 1099;
        static Registry registry;
        static int job_nums = 0;
	private HashMap<Integer, ArrayList<Integer>> job_status;
        public JobTracker() throws RemoteException {
		job_status = new HashMap<Integer, ArrayList<Integer>>();
	}
        public static void main(String args[]){
                try{

                        System.setProperty("java.rmi.server.hostname",host);
                        Registry registry = LocateRegistry.getRegistry(host, port);
                        JobTracker obj = new JobTracker();
                        registry.rebind("JobTracker", obj);
                }
                catch (Exception e){
                        System.out.println("err" + e.getMessage());
                        e.printStackTrace();
                }

        }
	public byte[] heartBeat(byte[] array) {

                try{
                        HeartBeatRequestMapReduce hb = HeartBeatRequestMapReduce.parseFrom(array);
                        int node_num = hb.getTaskTrackerId();
                        System.out.println("Recieved HeartBeat from: " + node_num);
                }
                catch (Exception e)
                {
                        System.out.println("Error sending the Heartbeat response");
//                        HeartBeatResponse.Builder hb_response = HeartBeatResponse.newBuilder();
  //                      hb_response.setStatus(0);
    //                    HeartBeatResponse array_response = hb_response.build();
      //                  return array_response.toByteArray();
                }
                String str = "core java api";
                byte[] b = str.getBytes();
 
		return b;
    }
        public byte[] getJobStatus(byte array[])
	{
		try{
			System.out.println("I am inside try");
			JobStatusRequest jsr = JobStatusRequest.parseFrom(array);
			int job_id = jsr.getJobId();
			System.out.println("well i moved forward: " + job_id);
			JobStatusResponse.Builder jsr_response = JobStatusResponse.newBuilder();
			jsr_response.setStatus(1);
			System.out.println("Just above the conditions");
                        System.out.println(job_status.get(job_id).get(0));
			if(job_status.get(job_id).get(1).equals(1)){
				jsr_response.setJobDone(true);
				System.out.println("In the correct condition");
			}
			else
			{
				System.out.println("In the correct else condition");
				jsr_response.setJobDone(false);
			}
			return jsr_response.build().toByteArray();	
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		JobStatusResponse.Builder jsr_response = JobStatusResponse.newBuilder();
                jsr_response.setStatus(0);
		return jsr_response.build().toByteArray();

	}
	public byte[] jobSubmit(byte array[])
	{
		try{
		JobSubmitRequest jsr = JobSubmitRequest.parseFrom(array);
		String mapper = jsr.getMapName();
		System.out.println("This is the mapper name received: "+ mapper);
		JobSubmitResponse.Builder jsr_response  = JobSubmitResponse.newBuilder();
		jsr_response.setStatus(1);
		jsr_response.setJobId(job_nums+1); 
		job_nums = job_nums + 1;
		ArrayList<Integer> temp = new ArrayList<Integer>();
		temp.add(0);//Adding the done status
		temp.add(5);//Adding total map_tasks
		temp.add(4);//Number of map tasks started
		temp.add(4);//Total reduce tasks
		temp.add(3);//Number of reduced tasks started
		job_status.put(job_nums,temp);
		return jsr_response.build().toByteArray();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		JobSubmitResponse.Builder jsr_response  = JobSubmitResponse.newBuilder();
                jsr_response.setStatus(0);
                return jsr_response.build().toByteArray();

	}


}
