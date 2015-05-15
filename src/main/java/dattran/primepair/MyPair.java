package dattran.primepair;

import java.io.Serializable;

@SuppressWarnings("serial")
public	class MyPair implements Serializable{
	int left;
	int right;
	public MyPair(int left, int right) {
		this.left = left;
		this.right = right;
	}
	public int getLeft() {
		return left;
	}
	public void setLeft(int left) {
		this.left = left;
	}
	public int getRight() {
		return right;
	}
	public void setRight(int right) {
		this.right = right;
	}
	@Override
	public String toString() {
		return "(" + left + "," + right + ")";
	}
	
	public boolean isPrimePair() {
		if (PrimePairUtils.isPrime(left) && PrimePairUtils.isPrime(right)) {
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof MyPair) {
			MyPair objPrimePair = (MyPair) obj;
			if (this.left == objPrimePair.left && this.right == objPrimePair.right) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return this.left * 1000 + this.right;
	}
}