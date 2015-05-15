package dattran.primepair;

public class PrimePairUtils {
	public static boolean isPrime(int number) {
		for (int i = 2 ; i < number/2; i++) {
			if (number % i == 0) {
				return false;
			}
		}
		return true;
	}
}
