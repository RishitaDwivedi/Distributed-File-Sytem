import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Dstore {
  private int port; // The port the Dstore listens to
  private static int cport; // The controllers port
  private int timeout; // Timeout in milliseconds
  private File fileFolder; // Where to store the data locally
  private ServerSocket dSSocket;
  private Socket cSocket;
  private BufferedReader cReader;
  private PrintWriter cWritter;

  public Dstore(int port, int cport, int timeout, String fileFolderName) {
    this.port = port;
    Dstore.cport = cport;
    this.timeout = timeout;

    this.fileFolder = new File(fileFolderName);
    if (fileFolder.exists() && !fileFolder.isDirectory()) {
      System.out.println("The folder name provided exists as a file and not a directory");
    } else if (!fileFolder.exists()) {
      System.out.println("New folder is being created");
      if (!fileFolder.mkdir()) {
        System.out.println("New folder could not be created");
      }
    }

    //send message to controller that a new dstore has been created
    try {
      cSocket = new Socket(InetAddress.getLocalHost(), cport);
      cReader = new BufferedReader(new InputStreamReader(cSocket.getInputStream()));
      cWritter = new PrintWriter(cSocket.getOutputStream(), true);
      //Join to controller
      System.out.println("A Dstore has joined the controller");
      //need to send port no. as well
      send(Protocol.JOIN_TOKEN + " " + port, cSocket);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    int port = Integer.parseInt(args[0]);
    int cport = Integer.parseInt(args[1]);
    int timeout = Integer.parseInt(args[2]);
    String fileFolder = args[3];

    try {
      Dstore dStore = new Dstore(port, cport, timeout, fileFolder);
      new Thread(dStore::listen).start();
      dStore.listenToController();
    } catch (Exception e) {
      System.out.println("Error creating the DStore for the fileFolder name");
      e.printStackTrace();
    }
  }

  private void listenToController() {
    try {
      BufferedReader in = cReader;
      String line;
      while ((line = in.readLine()) != null) {
        System.out.println("The Controller Message received: " + line);
        handleClientMessage(cSocket, line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  private void listen() {
    try {
      dSSocket = new ServerSocket(port);
      System.out.println("Waiting for a client to connect in Dstore");
      while (true) {
        Socket client = dSSocket.accept();
        new Thread (() -> {
          System.out.println("Client has been connected: " + client.getPort());
          try {
            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
              System.out.println("Client Message received: " + line);
              handleClientMessage(client, line);
            }
            client.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }).start();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void handleClientMessage(Socket client, String message) {
    //Messages that we can get from client: Store, Load, List, and Remove
    String[] splitMessage = message.split(" ");
    switch (splitMessage[0]) {
      case Protocol.STORE_TOKEN -> store(client, splitMessage[1], splitMessage[2]);
      case Protocol.REMOVE_TOKEN -> remove(client, splitMessage[1]);
      case Protocol.LOAD_DATA_TOKEN -> {
        try {
          OutputStream outputStream = client.getOutputStream();
          FileInputStream fileInputStream = new FileInputStream(
              fileFolder.getAbsolutePath() + "/" + splitMessage[1]);
          //        load(client, splitMessage[1]);
          byte[] buffer = new byte[1024];
          int bytes;
          while ((bytes = fileInputStream.read(buffer)) != -1) {
            System.out.println("Sending data");
            outputStream.write(buffer);
            outputStream.flush();
          }
          fileInputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      default -> System.out.println("Malformed message received: " + message);
    }
  }

  private synchronized void remove (Socket client, String filename) {
    System.out.println("Controller going to remove " + filename);

    //is this the right way
    File file = new File(fileFolder + File.separator + filename);
    if (file.delete()) {
      System.out.println("the file " + filename + " has been removed");
    }
    else {
      System.out.println(filename +" was not deleted ");
      return;
    }

    synchronized (cWritter) {
      String contAck = Protocol.REMOVE_ACK_TOKEN + " " + filename;
      cWritter.println(contAck);
    }
    System.out.println("The remove ack has been sent to controller");
  }

  //This method is to store a file to a dstore by creating a new file
  private synchronized void store(Socket client, String fileName, String fileSize) {
    String ack = Protocol.ACK_TOKEN;
    send(ack, client);

    //need to handle the file content sent to Dstore by client
    //need a buffered reader for that
    //need to create a new file and then need to writ eehat the buffered readrer read to the new file
    try {
      File newFile = new File(fileFolder + File.separator + fileName);
      if(newFile.createNewFile())
        System.out.println("File created");
      else
        System.out.println("File exists");
      FileWriter fileWriter = new FileWriter(newFile);
      BufferedReader dbuff = new BufferedReader(new InputStreamReader(client.getInputStream()));
      String line;
      while((line = dbuff.readLine()) != null) {
        System.out.println("File content is being recieved");
        fileWriter.write(line);
      }

      send(Protocol.STORE_ACK_TOKEN, client);

      synchronized (cWritter) {
        String contAck = Protocol.STORE_ACK_TOKEN + " " + fileName;
        cWritter.println(contAck);
      }
      System.out.println("The store ack has been sent to controller");
      fileWriter.close();
      dbuff.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
    //this is used to send messages to client
    //in rebalance will use to talk to dstores
    private void send(String message, Socket socket) {
    PrintWriter socketWriter = null;
    try {
      socketWriter = new PrintWriter(socket.getOutputStream());
    } catch (IOException e) {
      System.out.println("There was an error setting up the Printwritter for the message " + message);
      e.printStackTrace();
    }
      assert socketWriter != null;
      socketWriter.print(message);
    socketWriter.println();
    socketWriter.flush();
    System.out.println(message + " sent to " + socket.getPort());
  }
}
