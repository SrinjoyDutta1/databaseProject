package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

    private Iterator iter = null;
    private Predicate[] preds = null;
    //boolean hasNext;
    Tuple next = null;
    private boolean consumed;
    private boolean isOpen;
  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator aIter, Predicate... aPreds) {
    this.schema = aIter.getSchema();
    this.iter = aIter;
    this.preds = aPreds; 
    this.consumed = true;
    this.isOpen = true;
  }

  /**
   * Gives a one-line explanation of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  throw new UnsupportedOperationException("Not implemented");
    //Your code here

  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      iter.restart();
      consumed = true;

  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
    return this.iter.isOpen();

  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
    this.iter.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	//  throw new UnsupportedOperationException("Not implemented");
    //Your code here
    if (next!= null) return true;

    while (iter.hasNext()) {
      boolean success = false;
      Tuple tuple = iter.getNext();
      for (Predicate pred : preds) {
        if (pred.evaluate(tuple)) {
          success = true;
          break;
        }
      }
      if (success) {
        next = tuple;
        break;
      }
    }
    return next != null;


  }


  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      Tuple result = next;
      next = null;
      return result;
  }

} // public class Selection extends Iterator

