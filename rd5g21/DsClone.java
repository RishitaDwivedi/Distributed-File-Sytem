import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Objects;

public class DsClone {
//this needs to keep track of all the files saved and the port number of the dstore
  private final int dport;
  private BufferedReader read;
  private PrintWriter write;
  private final ArrayList<String> messagePool;
  private int noFiles;
  private final Socket dSocket;
  private boolean isAvailable;

  public DsClone(Socket socket, int dport) {
    this.dport = dport;
    this.dSocket = socket;
    this.isAvailable = true;
    messagePool = new ArrayList<>();
    noFiles = 0;

    try {
      read = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      write = new PrintWriter(socket.getOutputStream());
    } catch (IOException e) {
      e.printStackTrace();
    }
    new Thread(() ->{
      try {
        listen();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }).start();
  }
  //this method keeps listening for a message
  private void listen() throws IOException {
    //keep reading -> looking for a message
    //once youve read a message, send it to an array list "message pool"
    //once theres a message there the Controller
    for(;;) {
      String line;
      while ((line = read.readLine()) != null) {
        System.out.println("Message received inside the dstore clone: " + line);
        synchronized (messagePool) {
          messagePool.add(line);
        }
        System.out.println("\t" + messagePool);
      }
      isAvailable = false;
    }
  }
//need a recieve message - is going to keep looking at the message pool for a timeout span -> the controller timeout can use System.currentimr
  //need to keep repeating getMessage for x amount of time which the timeout time
  //if there is a message in message pool -> return that message?
  //if not return null

  public String recieveMessageClone(String expected) throws Exception {
    if (!isAvailable) throw new Exception("Dead store");
    System.out.println("Getting the message " + expected);
    System.out.println("Message pool has: " + messagePool);
    long time = Controller.timeout + System.currentTimeMillis();
    String message = null;
    while(time > System.currentTimeMillis() && message == null) {
      if (!isAvailable) throw new Exception("Dead store");
      message = getMessage(expected);
    }
//    messagePool.remove(expected);
    return message;
  }


  public synchronized String getMessage(String expectedMessage) throws Exception {
    //need to find a message inside thread pool that is the same as expectedMessage
    //therefore if the returned string is empty then the message doesnt exist in the message pool
    String returnMssg = null;
    synchronized (messagePool) {
      for (String s : messagePool) {
        if (!isAvailable) throw new Exception("Dead store");
        if (Objects.equals(s, expectedMessage)) {
          returnMssg = s;
        }
      }
    }

    return returnMssg;
  }

  public void setNoFiles(int noFiles) {
    this.noFiles = noFiles;
  }
  //the number of files contained in given dstore
  public int getNoFiles() {
    return noFiles;
  }

  public Socket getdSocket() {
    return dSocket;
  }

  public int getDport() {
    return dport;
  }
}
