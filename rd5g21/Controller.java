
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Controller {

  private int cport;//the controllers port
  private int replication;
  public static int timeout;
  private int rebalancePeriod;
  private ServerSocket server;

  private final Map<Integer , DsClone> dstores = new HashMap<>(); //contains clones of all the dstores added
  private final Map<String , Index> files = new HashMap<>(); //contains all of the files sent by clients and their respective Index objects
  private final Map<Socket , Integer> tries = new HashMap<>();

  public Controller(int cport, int replication, int timeout, int rebalancePeriod) {
    this.cport = cport;
    this.replication = replication;
    Controller.timeout = timeout;
    this.rebalancePeriod = rebalancePeriod;
  }

  public static void main(String[] args) {
    Integer cport = Integer.parseInt(args[0]);
    Integer replication = Integer.parseInt(args[1]);
    Integer timeout = Integer.parseInt(args[2]);
    Integer rebalancePeriod = Integer.parseInt(args[3]);
    Controller controller = new Controller(cport, replication, timeout, rebalancePeriod);
    controller.listen();
  }

  public int getTimeout() {
    return timeout;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }
  // listen listens for connections from client
  private void listen() {
    try {
      server = new ServerSocket(cport);

      while (true) {
        Socket client = server.accept();
        new Thread(() -> {
          System.out.println("Someone has connected... Checking if it is a dstore or a client");
          try {
            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            String line;
            if ((line = in.readLine()) != null) {
              String[] splitMessage = line.split(" ");
              if (splitMessage[0].equals(Protocol.JOIN_TOKEN)){
                System.out.println("It is a Dstore");
                dstoreJoins(splitMessage[1], client);
              } else {
                System.out.println("It is a client");
                System.out.println("Message received: " + line);
                handleClientMessage(client, line);
                listenToClient(in, client);
              }
            }
          } catch (IOException e) {
            System.err.println("There was an error establishing the Buffered reader for client " + client.getPort());
            e.printStackTrace();
          }
        }).start();
      }
    } catch (IOException e) {
      System.out.println("There was an error setting up the server socket in the controller");
      e.printStackTrace();
    }
  }

  private void listenToClient(BufferedReader in, Socket client) {
    String mssg = "";
    try {
      while ((mssg = in.readLine()) != null) {
        System.out.println("Message received " + mssg);
        handleClientMessage(client, mssg);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void recieveMessage(Socket client, String messageRecieved) {
    System.out.println("The message " + messageRecieved + "has been recieved by" + client);
  }
  //this handles messages sent by client or dstores
  private void handleClientMessage(Socket client, String message) {
    recieveMessage(client, message);
    //Messages that we can get from client: Store, Load, List, and Remove
    String[] splitMessage = message.split(" ");
    switch (splitMessage[0]) {
      case Protocol.STORE_TOKEN ->
          //      Do the store operation here
          store(client, splitMessage[1], splitMessage[2]);
      case Protocol.LOAD_TOKEN -> load(client, splitMessage[1]);
      case Protocol.LIST_TOKEN -> list(client);
      case Protocol.REMOVE_TOKEN -> remove(client, splitMessage[1]);
      case Protocol.RELOAD_TOKEN -> reload(client, splitMessage[1]);
      default -> System.out.println("Malformed message received: " + message);
    }
  }

  private void load(Socket client, String filename) {
    //storby is an arraylist of integers i need to get a random integer from that list
    synchronized (tries) {
      tries.put(client , 0 );
    }
    synchronized (files) {
      if (!files.containsKey(filename)) {
        send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN, client);
        return;
      }
      System.out.println("the file is stored by these dstores " + files.get(filename).getStoreBy());
      ArrayList<Integer> dports = new ArrayList<>();
      dports = files.get(filename).getStoreBy();
      //selected clone will be dports.get(0)
      String message = Protocol.LOAD_FROM_TOKEN + " " + dports.get(0) + " " + (files.get(filename)
          .getFilesize());
      send(message , client);
    }
  }

  private void reload(Socket client , String filename) {
    synchronized (tries) {
      tries.replace(client , tries.get(client) + 1);
      if(tries.get(client) >= replication) {
        send(Protocol.ERROR_LOAD_TOKEN , client);
        return;
      }
    }

    ArrayList<Integer> dports;
    synchronized (files) {
      if (!files.containsKey(filename)) {
        send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN, client);
        return;
      }
      System.out.println("the file is stored by these dstores " + files.get(filename).getStoreBy());
      dports = new ArrayList<>();
      dports = files.get(filename).getStoreBy();
    }
    //selected clone will be dports.get(0)
    String message = Protocol.LOAD_FROM_TOKEN + " " +  dports.get(0) + " " + (files.get(filename).getFilesize());
    send(message , client);
  }

  private synchronized void remove(Socket client, String filename) {
    //update index to remove in progress
    Index index;
    synchronized (files) {
      index = files.get(filename);
    }
    if (index == null) {
      send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN , client);
      return;
    }
    index.removeProgress();
    for (int i = 0; i < index.getStoreBy().size(); i ++) {
      //need to send a message to dstore to remove the file
      String message;
      message = Protocol.REMOVE_TOKEN + " " + filename;
      System.out.println("Sending remove to dstore " + dstores.get(index.getStoreBy().get(i)).getDport());
      send(message , dstores.get(index.getStoreBy().get(i)).getdSocket());//this will send to the dsclone
    }

    CountDownLatch latchForACKs = new CountDownLatch(index.getStoreBy().size());

    //get an array list of dsCLones from their port no
    ArrayList<DsClone> dsClonesStore = new ArrayList<>();
    for(int j = 0 ; j < index.getStoreBy().size(); j++) {
      synchronized (dstores) {
        dsClonesStore.add(dstores.get(index.getStoreBy().get(j)));
      }
    }

    // for a remove ack from said dstore.
    for(DsClone dsClone :  dsClonesStore) {
      new Thread (() -> {
        String str = null;
        try {
          if (dsClone.recieveMessageClone(Protocol.REMOVE_ACK_TOKEN + " " + filename) != null) {
            System.out.println("Received the store ACK counting down the CountDownLatch");
            synchronized (index) {
              index.removeComplete();
            }
            latchForACKs.countDown();
          }
          else {
            System.out.println("Message pool couldnt find " + Protocol.REMOVE_ACK_TOKEN + " " + filename);
          }
        } catch (Exception e) {
          System.out.println("Store is dead");
          synchronized (dstores) {
            dstores.remove(dsClone.getDport());
          }
          synchronized (files) {
            files.forEach((s, index1) -> {
              if (index.getStoreBy().contains(dsClone.getDport())) {
                index1.getStoreBy().remove(dsClone.getDport());
              }
            });
          }
        }
      }).start();
    }

    // THe await method will execute the if statement when the countdown latch has reached zero. If
    // the latch has not reached zero before the timeout happens then the else will be executed.
    try {
      if (latchForACKs.await(timeout, TimeUnit.MILLISECONDS)) {
        send(Protocol.REMOVE_COMPLETE_TOKEN, client);
        System.out.println("Removing was successful");
        synchronized (files) {
          files.remove(filename);
        }
      } else {
        System.out.println("Storing was unsuccessful");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  //this method is for the list sent by client
  private void list(Socket client) {
    StringBuilder fileNames = new StringBuilder(Protocol.LIST_TOKEN);

    for(int i = 0; i < files.size(); i++) {
      Object key = files.keySet().toArray()[i];
      fileNames.append(" ").append((files.get(key)).getFilename());

    }
    //will the toString() work for this
    //[item1,item2,item3] ... List item1 item2 item3
    send(fileNames.toString() , client);
  }



  //this method creates a dstore clone for a new dstore joined and its it to the hashmap dstores
  private synchronized void dstoreJoins(String dport, Socket client) {
    //create a dstore clone and put it inside the hashmap
    System.out.println("adding dstore to list " +dport);
    dstores.put(Integer.valueOf(dport) , new DsClone(client, Integer.parseInt(dport)));
    System.out.println("the dstores are now " + dstores );
  }
  //this method is the store operation
  private void store(Socket client, String fileName, String fileSize) {
    //if inside of files this filename already exists, send file already exists
    synchronized (files) {
    if(files.containsKey(fileName)) {
      send(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN, client);
      return;
    }
    }


    List<DsClone> sortedList = new ArrayList<>(dstores.values());
    Collections.sort(sortedList, new Comparator<DsClone>() {
      @Override
      public int compare(DsClone o1, DsClone o2) {
        if (o1.getNoFiles() > o2.getNoFiles())
          return -1;
        else if (o1.getNoFiles() < o2.getNoFiles())
          return 1;
        else
          return 0;
      }
    });

    StringBuilder messageToSend = new StringBuilder(Protocol.STORE_TO_TOKEN);
    ArrayList<DsClone> chosenDstores = new ArrayList<>();
    System.out.println("picking dstores to store from " + dstores);
    for(int i = 0; i < replication; i++) {
      messageToSend.append(" ").append(dstores.keySet().toArray()[i]);
      chosenDstores.add((DsClone) dstores.values().toArray()[i]);
    }
    send(messageToSend.toString() , client);

    Index index = new Index(Integer.parseInt(fileSize) , fileName);
    index.storeProgress();
    CountDownLatch latchForACKs = new CountDownLatch(chosenDstores.size());

    // For each dstore we have chosen to store the files to we create a new thread that will be waiting
    // for a store ack from said dstore.
    ArrayList <Integer> dports = new ArrayList<>();
    for(DsClone dsClone : chosenDstores) {
      new Thread (() -> {
        String str = null;
        synchronized (dsClone) {
          try {
            if (dsClone.recieveMessageClone(Protocol.STORE_ACK_TOKEN + " " + fileName) != null) {
              dports.add(dsClone.getDport());
              System.out.println("Received the store ACK counting down the CountDownLatch");
              index.storeComplete();
              latchForACKs.countDown();
            }
            else {
              System.out.println("Message pool couldnt find " + Protocol.STORE_ACK_TOKEN + " " + fileName);
            }
          } catch (Exception e) {
            System.out.println("Dstore is dead");
            synchronized (dstores) {
              dstores.remove(dsClone.getDport());
            }
            synchronized (files) {
              files.forEach((s, index1) -> {
                if (index.getStoreBy().contains(dsClone.getDport())) {
                  index1.getStoreBy().remove(dsClone.getDport());
                }
              });
            }
          }
        }
      }).start();
    }

    // THe await method will execute the if statement when the countdown latch has reached zero. If
    // the latch has not reached zero before the timeout happens then the else will be executed.
    try {
      if (latchForACKs.await(timeout, TimeUnit.MILLISECONDS)) {
        send(Protocol.STORE_COMPLETE_TOKEN, client);
        System.out.println("Storing was successful");
        //index.setStatus(Status.STORE_COMPLETE);
        synchronized (index) {
          index.setStoreBy(dports);

        }
        synchronized (files) {
          files.put(fileName , index);
        }
      } else {
        System.out.println("Storing was unsuccessful");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  //this a helper method for store to send a message to the client, can be used for other operations - maybe
  private void send(String message, Socket socket) {
    PrintWriter socketWriter = null;
    try {
      socketWriter = new PrintWriter(socket.getOutputStream());
    } catch (IOException e) {
      System.out.println("There was an error setting up the Printwritter for the message " + message);
    }
    socketWriter.print(message);
    socketWriter.println();
    socketWriter.flush();
    System.out.println(message + " sent to " + socket.getPort());
  }
}