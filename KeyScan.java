package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {
    private HeapFile file = null; // needed for restart(), getFile();
    private HashScan scan = null;
    private boolean isOpen;
    private HashIndex h;
    private SearchKey searchKey;
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema aSchema, HashIndex aIndex, SearchKey aKey, HeapFile aFile) {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      this.schema = aSchema;
      this.file = aFile;
      this.scan =  aIndex.openScan(aKey);
      isOpen = true;
      this.h = aIndex;
      this.searchKey = aKey;
  }

  /**
   * Gives a one-line explanation of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  //throw new UnsupportedOperationException("Not implemented");
      scan.close();

      scan = this.h.openScan(this.searchKey);
    //Your code here
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
	  //throw new UnsupportedOperationException("Not implemented");
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
//      if (!scan.hasNext()) {
//        throw new IllegalStateException();
//      }

      RID rid = scan.getNext();
      byte[]arr = this.file.selectRecord(rid);
      return new Tuple(schema, arr);
  }

} // public class KeyScan extends Iterator

