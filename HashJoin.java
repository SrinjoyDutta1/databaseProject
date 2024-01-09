package relop;

import heap.HeapFile;
import index.HashIndex;
import global.SearchKey;
import global.RID;
import global.AttrOperator;
import global.AttrType;

import java.util.ArrayList;

public class HashJoin extends Iterator {


	private IndexScan is1;
	private IndexScan is2;
	private Tuple leftTuple;
	private boolean startJoin = true;
	private ArrayList<Tuple> resultTuples= new ArrayList<>();
	private int bucket1 = -1;
	private int bucket2;
	private int joinCol1;
	private	 int joinCol2;
	private int counter = 0;
// boolean variable to indicate whether the pre-fetched tuple is consumed or not

	// pre-fetched tuple
	private Tuple nextTuple;
	public HashJoin(Iterator aIter1, Iterator aIter2, int aJoinCol1, int aJoinCol2){
//		this.first = aIter1;
		this.joinCol1 = aJoinCol1;
		this.joinCol2 = aJoinCol2;
		HeapFile hf1 = new HeapFile(null);
		HashIndex hi1 = new HashIndex(null);
		while(aIter1.hasNext()){
			Tuple t = aIter1.getNext();
			RID rid = t.insertIntoFile(hf1);
			Object val = t.getField(aJoinCol1);
			SearchKey key = new SearchKey(val);
			hi1.insertEntry(key, rid);
		}
		is1 = new IndexScan(aIter1.schema, hi1, hf1);
//		this.second = aIter2;
		HeapFile hf2 = new HeapFile(null);
		HashIndex hi2 = new HashIndex(null);
		while(aIter2.hasNext()){
			Tuple t = aIter2.getNext();
			RID rid = t.insertIntoFile(hf2);
			Object val = t.getField(aJoinCol2);
			SearchKey key = new SearchKey(val);
			hi2.insertEntry(key, rid);
		}
		is2 = new IndexScan(aIter2.schema, hi2, hf2);
		this.schema = Schema.join(aIter1.getSchema(), aIter2.getSchema());
    aIter1.close();
    aIter2.close();
	}



	@Override
	public void explain(int depth) {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
	}

	@Override
	public void restart() {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
		this.is1.restart();
		this.is2.restart();
	}

	@Override
	public boolean isOpen() {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
		return this.is1.isOpen() && this.is2.isOpen();
	}

	@Override
	public void close() {
//		throw new UnsupportedOperationException("Not implemented");
		//Your code here
		this.is1.close();
		this.is2.close();
	}

	//@Override
	public boolean hasNext1() {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
		if (!resultTuples.isEmpty()) return true;
		int b1 = this.is1.getNextHash();
		if(this.bucket1 == -1){
			this.bucket1 = b1;
		}
		HashTableDup hashTableDup = new HashTableDup();
		int b2 = this.is2.getNextHash();
		while(b1 == this.bucket1 && is1.hasNext()) {
			Tuple t1 = is1.getNext();
//			Object val = t1.getField(this.joinCol1);
//			SearchKey key = new SearchKey(val);
			hashTableDup.add(is1.getLastKey(),t1);
			b1 = this.is1.getNextHash();
		}

		if(b2 > this.bucket1 && b1 == b2){
			hashTableDup.clear();
			this.bucket1 = b1;
			while(b1 == this.bucket1) {
				Tuple t1 = is1.getNext();
//				Object val = t1.getField(this.joinCol1);
//				SearchKey key = new SearchKey(val);
				hashTableDup.add(is1.getLastKey(),t1);
				b1 = this.is1.getNextHash();
			}
		}
		while (b2 == this.bucket1 && is2.hasNext()) {
			Tuple tuple = is2.getNext();
//			Object val = tuple.getField(this.joinCol2);
//			SearchKey key = new SearchKey(val);
			if (hashTableDup.containsKey(is2.getLastKey())) {
				Tuple[]arr =  hashTableDup.getAll(is2.getLastKey());
				for (Tuple t :arr) {
					Tuple joined = Tuple.join(t, tuple, this.schema);
					this.resultTuples.add(joined);
				}
			}
			b2 = this.is2.getNextHash();
		}
		this.bucket1 = b1;
		return !resultTuples.isEmpty();

	}
	@Override
	public boolean hasNext() {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
		if (!resultTuples.isEmpty()) return true;
		if (!is1.hasNext()) return false;
		int b1 = is1.getNextHash();
		int b2 = is2.getNextHash();
		if (b1 > b2) {
			while (b1 !=  b2) {
				is2.getNext();
				b2 = is2.getNextHash();
			}
		} else if (b1 < b2) {
			while (b1 !=  b2) {
				is1.getNext();
				b1 = is1.getNextHash();
			}
		}
		HashTableDup hashTableDup = new HashTableDup();
		this.bucket1 = b1;
		while(b1 == this.bucket1 && is1.hasNext()) {
			Tuple t1 = is1.getNext();
			hashTableDup.add(is1.getLastKey(),t1);
			b1 = this.is1.getNextHash();
		}
		while (b2 == this.bucket1 && is2.hasNext()) {
			Tuple tuple = is2.getNext();
			if (hashTableDup.containsKey(is2.getLastKey())) {
				Tuple[]arr =  hashTableDup.getAll(is2.getLastKey());
				for (Tuple t :arr) {
					Tuple joined = Tuple.join(t, tuple, this.schema);
					this.resultTuples.add(joined);
				}
			}
			b2 = this.is2.getNextHash();
		}

		return !resultTuples.isEmpty();


	}

	@Override
	public Tuple getNext() {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here



		Tuple next = this.resultTuples.get(counter);
		this.counter++;
		if (counter > this.resultTuples.size()-1) {
			this.resultTuples.clear();

			this.counter = 0;
		}
		if (counter == 0) {
			hasNext();
		}
		return next;

	}
} // end class HashJoin;


