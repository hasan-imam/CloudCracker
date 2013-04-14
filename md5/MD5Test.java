import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

public class MD5Test {
	private static BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	public static void main(String[] args){

		System.out.println("Sample word: r41nb0w");
		//        String word = "r41nb0w";
		//        String hash = getHash(word);
		//        System.out.println(hash); 
		while (true) {
			try {
				// Read command
				System.out.print(">> ");
				String word = br.readLine();
				String hash = getHash(word);
				System.out.println(hash); 
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static String getHash(String word) {

		String hash = null;
		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			BigInteger hashint = new BigInteger(1, md5.digest(word.getBytes()));
			hash = hashint.toString(16);
			while (hash.length() < 32) hash = "0" + hash;
		} catch (NoSuchAlgorithmException nsae) {
			// ignore
		}
		return hash;
	}
}
