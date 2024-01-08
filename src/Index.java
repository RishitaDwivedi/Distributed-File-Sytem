import java.util.ArrayList;

// A new index object is created for every new file created and it stores that files details
public class Index {
  private String filename;
  private long filesize;
  //Integer is the port no. of the dstore storing this file.
  private ArrayList<Integer> storeBy = new ArrayList<>();
  private Status status;

  public static enum Status {
    STORE_IN_PROGRESS,
    STORE_COMPLETE,
    REMOVE_IN_PROGRESS,
    REMOVE_COMPLETE
  }

  public Index(long filesize, String filename) {
    this.status = Status.STORE_IN_PROGRESS;
    this.filesize = filesize;
    this.filename = filename;
  }

  public void setStoreBy(ArrayList<Integer> storeBy) {
    this.storeBy = storeBy;
  }

  public ArrayList<Integer> getStoreBy() {
    return storeBy;
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public void storeProgress() {
    status = Status.STORE_IN_PROGRESS;
  }

  public void storeComplete() {
    status = Status.STORE_COMPLETE;
  }
  public void removeProgress() {
    status = Status.REMOVE_IN_PROGRESS;
  }
  public void removeComplete() {
    status = Status.REMOVE_COMPLETE;
  }

  public long getFilesize() {
    return filesize;
  }

  public void setFilesize(long filesize) {
    this.filesize = filesize;
  }
}
