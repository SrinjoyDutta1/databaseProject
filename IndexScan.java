package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import heap.HeapScan;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {
    private HeapFile file = null; // needed for restart(), getFile();
    private BucketScan scan = null;
    private RID rid = null;
    private boolean isOpen;
    private HashIndex h;

  /**
   * Constructs an index scan, given the hash index and schema.
   */

  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      this.schema = schema;
      this.file = file;
      this.scan = index.openScan();
      this.rid = new RID();
      isOpen = true;
      this.h = index;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      for(int i = 0; i < depth; i++){
          System.out.printf("    ");
      }
      System.out.printf("IndexScan\n");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      scan.close();

      scan = this.h.openScan();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      return this.isOpen;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	 // throw new UnsupportedOperationException("Not implemented");
  //Your code here
    scan.close();
    isOpen = false;

  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      return scan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      if (!this.hasNext()) {
          throw new IllegalStateException();
      }
      RID rid = this.scan.getNext();
      byte[]arr = file.selectRecord(rid);
      return new Tuple(schema, arr);
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      return scan.getLastKey();
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      return scan.getNextHash();
  }

} // public class IndexScan extends Iterator

