


public class MapPool implements Runnable {


    private String map_command;

    public MapPool(String cmd){
        map_command = cmd;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+ "Start. Command = " + map_command);
        processCommand();
        System.out.println(Thread.currentThread().getName()+ " End.");
    }

    private void processCommand() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
